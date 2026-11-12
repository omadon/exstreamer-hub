# Exstreamer Hub
    docker compose up -d --build   →  http://<server>:8080
Podaci (uređaji, grupe, playliste, MP3) su u ./data. Mikrofon u pregledniku traži HTTPS (ili localhost) – stavi reverse proxy (Caddy/nginx).
Aplikacija nema prijavu: izloži je samo u LAN-u ili iza proxyja s autentikacijom.
