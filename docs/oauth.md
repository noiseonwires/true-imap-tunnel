# OAuth2 / XOAUTH2 authentication

`true-imap-tunnel` can authenticate to IMAP servers with SASL XOAUTH2. This is
required by providers such as Outlook.com, Microsoft 365, and Gmail that no
longer accept normal IMAP passwords for many accounts.

The tunnel itself does not implement every provider's OAuth login flow. Instead,
each account can either contain a static short-lived `oauth2_token`, or an
`oauth2_token_command` that prints a fresh access token on stdout.

## Outlook.com / Microsoft 365 helper

The repository includes a small Microsoft-specific helper:

```text
tools\outlook_oauth2_token.py
```

It is project-local code that uses Microsoft's `msal` Python package. It is not a
generic OAuth2 script and it is not downloaded from another mail client.

Install the Python dependency once:

```powershell
python -m pip install --user msal
```

Initialize the token cache once per mailbox:

```powershell
python .\tools\outlook_oauth2_token.py --username "user@outlook.com"
```

The helper prints a Microsoft device-code login instruction. Open the shown URL,
enter the code, and complete sign-in. After that, silent mode should return a
token without interaction:

```powershell
python .\tools\outlook_oauth2_token.py --username "user@outlook.com" --no-interactive
```

The default settings are intended for ordinary personal Outlook.com users:

- IMAP scope: `https://outlook.office.com/IMAP.AccessAsUser.All`
- Tenant: `consumers`
- Client ID: a public desktop-mail OAuth client ID

For Microsoft 365 work/school accounts, try `--tenant common` or the tenant ID:

```powershell
python .\tools\outlook_oauth2_token.py --username "user@company.com" --tenant common
```

Use the same `--tenant` in the YAML `oauth2_token_command`.

## YAML configuration

Use `oauth2_token_command` instead of `password`. The command is executed on
every IMAP reconnect, so the helper can refresh expired access tokens.

Client side:

```yaml
mode: client
listen: "127.0.0.1:2222"

accounts:
  - name: "outlook"
    host: "outlook.office365.com:993"
    username: "user@outlook.com"
    oauth2_token_command: 'python tools\outlook_oauth2_token.py --username "user@outlook.com" --no-interactive'
    tls: "implicit"
    folder_send: "TunnelC2S"
    folder_recv: "TunnelS2C"
```

Server side uses the same account settings, but swaps the send/receive folders:

```yaml
mode: server
target: "127.0.0.1:10000"

accounts:
  - name: "outlook"
    host: "outlook.office365.com:993"
    username: "user@outlook.com"
    oauth2_token_command: 'python tools\outlook_oauth2_token.py --username "user@outlook.com" --no-interactive'
    tls: "implicit"
    folder_send: "TunnelS2C"
    folder_recv: "TunnelC2S"
```

On Windows, prefer the unquoted relative script path shown above. The tunnel
runs `oauth2_token_command` through `cmd /C`, and quoted relative paths such as
`".\tools\outlook_oauth2_token.py"` can be parsed incorrectly.

## What the tunnel expects from a token command

Any provider-specific helper can be used if it follows this contract:

1. Print exactly one usable OAuth2 access token to stdout.
2. Exit with status code `0` on success.
3. Print diagnostics to stderr, not stdout.
4. Exit non-zero if a token cannot be obtained.
5. Return within 30 seconds.
6. Cache/refresh tokens itself if the provider uses refresh tokens.

The tunnel trims whitespace from stdout and uses the result as the bearer token
in the XOAUTH2 initial response:

```text
user=<username>^Aauth=Bearer <access-token>^A^A
```

`^A` is byte `0x01`. The tunnel builds this XOAUTH2 payload internally; the
helper must output only the raw access token, not base64 and not the full
XOAUTH2 string.

## Implementing other providers

To add a helper for another provider, create a script that performs that
provider's OAuth2 flow, stores refresh credentials safely, and prints a valid
IMAP access token on demand.

Provider-specific values usually differ:

| Provider | Typical IMAP host | Token scope |
| --- | --- | --- |
| Outlook.com / Microsoft 365 | `outlook.office365.com:993` | `https://outlook.office.com/IMAP.AccessAsUser.All` |
| Gmail | `imap.gmail.com:993` | `https://mail.google.com/` |

For example, a future Gmail helper could be configured like this:

```yaml
accounts:
  - name: "gmail"
    host: "imap.gmail.com:993"
    username: "user@gmail.com"
    oauth2_token_command: 'python tools\gmail_oauth2_token.py --username "user@gmail.com" --no-interactive'
    tls: "implicit"
    folder_send: "TunnelC2S"
    folder_recv: "TunnelS2C"
```

The helper would still only need to print the raw access token. The tunnel does
not care how the helper obtained it.

You can also use an external OAuth credential manager instead of writing a new
helper, for example `oama`, `pizauth`, or `mutt_oauth2.py`, as long as the
configured command prints a raw access token to stdout.

## Static tokens

For quick experiments, you can paste a short-lived access token directly:

```yaml
oauth2_token: "eyJ..."
```

This is not recommended for normal use because access tokens expire quickly and
should not be committed or shared. Prefer `oauth2_token_command`.
