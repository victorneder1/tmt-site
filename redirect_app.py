"""
Redirect-only WSGI app for btgtmt.com while the full site is parked.

start.sh serves this instead of app.py when the REDIRECT_URL variable is set on
Railway. Every request gets a temporary 302 to REDIRECT_URL (302, not 301, so
browsers do not cache it and deleting the variable brings the full site back).
Railway's healthcheck (Host: healthcheck.railway.app, path /telecom/) gets a 200
so deploys still pass; visitors opening /telecom/ are redirected like any page.
"""

import os

REDIRECT_URL = os.environ["REDIRECT_URL"].strip()
HEALTHCHECK_HOST = "healthcheck.railway.app"


def app(environ, start_response):
    host = environ.get("HTTP_HOST", "").split(":")[0].lower()
    if host == HEALTHCHECK_HOST:
        start_response("200 OK", [("Content-Type", "text/plain"), ("Cache-Control", "no-store")])
        return [b"ok (redirect mode)"]
    start_response("302 Found", [("Location", REDIRECT_URL), ("Cache-Control", "no-store"), ("Content-Length", "0")])
    return [b""]
