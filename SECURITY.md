# Security policy

This project is an educational distributed-systems implementation and is not hardened for production or untrusted networks.

The client API has no authentication or authorization. Internal Raft RPCs have no mutual authentication or transport encryption. Do not expose the service directly to the public internet or use it to store sensitive data.

Please report security concerns privately through GitHub's security-advisory interface rather than a public issue. Include affected commit(s), reproduction steps, and impact. There is currently no supported production release line or guaranteed response window.
