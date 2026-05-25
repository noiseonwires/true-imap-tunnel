#!/usr/bin/env python3
"""Print a Microsoft OAuth2 access token suitable for IMAP XOAUTH2.

Run once without --no-interactive to initialize the MSAL cache via device code:

    python .\tools\outlook_oauth2_token.py --username user@outlook.com

After that, true-imap-tunnel can call the same command with --no-interactive.
"""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

try:
    import msal
except ImportError:
    print(
        "Missing dependency: install with `python -m pip install --user msal`.",
        file=sys.stderr,
    )
    raise SystemExit(2)


DEFAULT_IMAP_SCOPE = "https://outlook.office.com/IMAP.AccessAsUser.All"
DEFAULT_MICROSOFT_CLIENT_ID = "9e5f94bc-e8a4-4e73-b8be-63364c29d753"


def default_cache_path() -> Path:
    local_app_data = os.environ.get("LOCALAPPDATA")
    if local_app_data:
        return Path(local_app_data) / "true-imap-tunnel" / "outlook-msal-cache.json"

    cache_home = os.environ.get("XDG_CACHE_HOME")
    if cache_home:
        return Path(cache_home) / "true-imap-tunnel" / "outlook-msal-cache.json"

    return Path.home() / ".cache" / "true-imap-tunnel" / "outlook-msal-cache.json"


def load_cache(path: Path) -> msal.SerializableTokenCache:
    cache = msal.SerializableTokenCache()
    if path.exists():
        cache.deserialize(path.read_text(encoding="utf-8"))
    return cache


def save_cache(cache: msal.SerializableTokenCache, path: Path, *, force: bool = False) -> None:
    if not force and not cache.has_state_changed:
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(cache.serialize(), encoding="utf-8")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Print a Microsoft access token for Outlook/Office 365 IMAP XOAUTH2."
    )
    parser.add_argument(
        "--username",
        required=True,
        help="Mailbox login, for example user@outlook.com.",
    )
    parser.add_argument(
        "--client-id",
        default=DEFAULT_MICROSOFT_CLIENT_ID,
        help=(
            "Microsoft OAuth public client ID. Defaults to a public desktop-mail "
            "client ID so personal Outlook.com users do not need Azure access."
        ),
    )
    parser.add_argument(
        "--tenant",
        default="consumers",
        help='Microsoft tenant: "consumers" for personal Outlook.com accounts, "common" for M365, or a tenant ID.',
    )
    parser.add_argument(
        "--scope",
        action="append",
        default=[DEFAULT_IMAP_SCOPE],
        help=f"OAuth scope to request. Defaults to {DEFAULT_IMAP_SCOPE}.",
    )
    parser.add_argument(
        "--cache",
        type=Path,
        default=default_cache_path(),
        help="MSAL token cache path.",
    )
    parser.add_argument(
        "--no-interactive",
        action="store_true",
        help="Do not start device-code login; fail if no cached/refreshable token exists.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    scopes = list(dict.fromkeys(args.scope))
    authority = f"https://login.microsoftonline.com/{args.tenant}"

    cache = load_cache(args.cache)
    app = msal.PublicClientApplication(
        client_id=args.client_id,
        authority=authority,
        token_cache=cache,
    )

    accounts = app.get_accounts(username=args.username)
    if not accounts:
        all_accounts = app.get_accounts()
        if len(all_accounts) == 1:
            accounts = all_accounts

    result = None
    for account in accounts:
        result = app.acquire_token_silent(scopes, account=account)
        if result and "access_token" in result:
            break

    if not result or "access_token" not in result:
        if args.no_interactive:
            cached = ", ".join(
                account.get("username", "<unknown>") for account in app.get_accounts()
            )
            cached_hint = f" Cached accounts: {cached}." if cached else ""
            print(
                "No cached Microsoft access token. Run this command once without "
                "--no-interactive and complete the device-code login. "
                f"Cache path: {args.cache}.{cached_hint}",
                file=sys.stderr,
            )
            return 1

        flow = app.initiate_device_flow(scopes=scopes)
        if "user_code" not in flow:
            print(f"Failed to start device-code flow: {flow}", file=sys.stderr)
            return 1

        print(flow["message"], file=sys.stderr)
        result = app.acquire_token_by_device_flow(flow)

    access_token = result.get("access_token") if result else None
    if not access_token:
        error = result.get("error") if result else "unknown_error"
        description = result.get("error_description") if result else "No result returned."
        correlation_id = result.get("correlation_id") if result else "n/a"
        print(
            f"Failed to acquire Microsoft access token: {error}: {description} "
            f"(correlation_id={correlation_id})",
            file=sys.stderr,
        )
        return 1

    save_cache(cache, args.cache, force=True)
    print(access_token)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
