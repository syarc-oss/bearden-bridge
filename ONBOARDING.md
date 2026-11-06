# Onboarding a New Person to the Bear Den Mesh Bridge

How to get a new person chatting on the LoRa mesh from Matrix (and XMPP).
Replaces the need to remember any of this. Server details as of 2026-10-11.

## Environment facts

| Thing | Value |
|---|---|
| Homeserver | `matrix.example.net` (port 8008) |
| Server software | Tuwunel (systemd unit `tuwunel`, config `/etc/tuwunel/tuwunel.toml`) |
| Registration token | `reg_<your-registration-token>` — set in `registration_token` in the toml |
| Bridge bot | `@bearden:matrix.example.net` (creds held by the operator) |
| Bridge room | `#mesh:matrix.example.net` (private; internal room id) |
| Bridge service | systemd `bearden-bridge`, API on port 8095, admin token `<your-admin-token>` |
| Client | Element (any platform) |

## Steps (Matrix path)

1. **Give them the registration token** out-of-band (it's the only gate).

2. **They install Element and sign up:**
   - Homeserver URL: `http://matrix.example.net:8008`
   - Choose username + password; when asked for a *registration token*, paste
     the token from your homeserver config.
   - Their id becomes `@<username>:matrix.example.net`.

   Their device needs `matrix.example.net` to resolve (LAN DNS or split-horizon
   DNS).

3. **Invite them to the room.** The room is private, so the bot must invite.
   Run on the bridge host (as any user):

   ```bash
   python3 - <<'EOF'
   import json,urllib.request
   HS="http://matrix.example.net:8008"
   tok="<bot access token>"
   room="!<room-id>:matrix.example.net"
   them="@newperson:matrix.example.net"
   req=urllib.request.Request(HS+f"/_matrix/client/r0/rooms/{room}/invite",
       method="POST", data=json.dumps({"user_id":them}).encode())
   req.add_header("Content-Type","application/json")
   req.add_header("Authorization","Bearer "+tok)
   print(urllib.request.urlopen(req).status)
   EOF
   ```

   (Or from Element logged in as the bot, open the room -> invite.)

4. **They accept the invite** (Element shows it under Invites / the + menu).

5. **First post will bounce** with `newperson: not sent (user not approved)`.
   Approve them on the bridge host:

   ```bash
   curl -X POST -H "Authorization: Bearer <your-admin-token>" \
     http://127.0.0.1:8095/users/matrix/<username>/approve
   ```

   (console Chat Link app does this too, once it speaks the matrix backend).
   Their next message goes out over LoRa; mesh traffic appears as
   `[mesh] <name>: <text>` in the room.

## XMPP path (legacy)

Prosody `xmpp.example.net`, room `mesh@conference.xmpp.example.net`, bot nick
`bridge`. New users register via the console Chat Link app, which provisions
the account and vouches it in one step. Identity = room nick.

## Notes

- Bridge identity model: users are keyed `<backend>:<backend_uid>`; for Matrix
  the uid is the localpart (e.g. `user1` for `@user1:matrix.example.net`).
- Moving the homeserver later: user/room ids embed the server name, so plan
  `.well-known` delegation before any real migration.
- Bridge logs: `journalctl -u bearden-bridge -f`; homeserver logs:
  `journalctl -u tuwunel -f`.
