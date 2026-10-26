#!/usr/bin/env python3
"""Fail closed unless the requested immutable image SHA tag is confirmed absent."""

import base64
import json
import os
import re
import sys
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import HTTPRedirectHandler, Request, build_opener

REGISTRY = "ghcr.io"
DEFAULT_REPOSITORY = "dzarlax-ai/rss-scheduler"
ALLOWED_REPOSITORIES = {
    DEFAULT_REPOSITORY,
    "dzarlax-ai/rss-job-worker",
    "dzarlax-ai/rss-article-ingestion",
    "dzarlax-ai/rss-article-processing",
    "dzarlax-ai/rss-web",
}
MANIFEST_ACCEPT = ", ".join((
    "application/vnd.oci.image.index.v1+json",
    "application/vnd.oci.image.manifest.v1+json",
    "application/vnd.docker.distribution.manifest.list.v2+json",
    "application/vnd.docker.distribution.manifest.v2+json",
))
TIMEOUT_SECONDS = 8
MAX_RESPONSE_BYTES = 64 * 1024


class RegistryCheckError(Exception):
    """A bounded error code that never includes registry or credential data."""


class _NoRedirect(HTTPRedirectHandler):
    def redirect_request(self, request, response, code, message, headers, new_url):
        return None


def _urlopen_no_redirect(request, timeout):
    return build_opener(_NoRedirect).open(request, timeout=timeout)


def _response(opener, request):
    try:
        response = opener(request, timeout=TIMEOUT_SECONDS)
    except HTTPError as error:
        try:
            body = error.read(MAX_RESPONSE_BYTES + 1)
        except (URLError, TimeoutError, OSError):
            raise RegistryCheckError("registry_transport_error") from None
        finally:
            error.close()
        return error.code, error.headers, body
    except (URLError, TimeoutError, OSError):
        raise RegistryCheckError("registry_transport_error") from None
    try:
        body = response.read(MAX_RESPONSE_BYTES + 1)
    except (URLError, TimeoutError, OSError):
        raise RegistryCheckError("registry_transport_error") from None
    finally:
        response.close()
    return response.status, response.headers, body


def _not_found_code(body):
    if len(body) > MAX_RESPONSE_BYTES:
        raise RegistryCheckError("registry_protocol_error")
    try:
        payload = json.loads(body)
    except (json.JSONDecodeError, UnicodeDecodeError):
        raise RegistryCheckError("registry_protocol_error") from None
    errors = payload.get("errors") if isinstance(payload, dict) else None
    if not isinstance(errors, list):
        return None
    for item in errors:
        if isinstance(item, dict) and item.get("code") in {"MANIFEST_UNKNOWN", "NAME_UNKNOWN"}:
            return item["code"]
    return None


def _challenge_parameters(value, repository):
    if not isinstance(value, str) or not value.startswith("Bearer "):
        raise RegistryCheckError("registry_auth_challenge_invalid")
    pairs = re.findall(r'([A-Za-z]+)="([^"\\]*)"', value[len("Bearer "):])
    parameters = dict(pairs)
    expected_scope = f"repository:{repository}:pull"
    if (
        parameters.get("realm") != f"https://{REGISTRY}/token"
        or parameters.get("service") != REGISTRY
        or parameters.get("scope") != expected_scope
    ):
        raise RegistryCheckError("registry_auth_challenge_invalid")
    return expected_scope


def _access_token(opener, username, password, scope):
    token_url = f"https://{REGISTRY}/token?{urlencode({'service': REGISTRY, 'scope': scope})}"
    credentials = base64.b64encode(f"{username}:{password}".encode()).decode("ascii")
    token_request = Request(token_url, headers={
        "Authorization": f"Basic {credentials}",
        "Accept": "application/json",
    })
    token_status, _, token_body = _response(opener, token_request)
    if token_status != 200 or len(token_body) > MAX_RESPONSE_BYTES:
        raise RegistryCheckError("registry_auth_error")
    try:
        token_payload = json.loads(token_body)
    except (json.JSONDecodeError, UnicodeDecodeError):
        raise RegistryCheckError("registry_auth_error") from None
    token = None
    if isinstance(token_payload, dict):
        token = token_payload.get("token") or token_payload.get("access_token")
    if (
        not isinstance(token, str)
        or not token
        or len(token) > MAX_RESPONSE_BYTES
        or not re.fullmatch(r"[A-Za-z0-9._~+/-]+=*", token)
    ):
        raise RegistryCheckError("registry_auth_error")
    return token


def tag_exists(image_tag, username, password, opener=_urlopen_no_redirect,
               repository=DEFAULT_REPOSITORY):
    if repository not in ALLOWED_REPOSITORIES:
        raise RegistryCheckError("registry_check_configuration_invalid")
    match = re.fullmatch(
        rf"ghcr\.io/{re.escape(repository)}:sha-([0-9a-f]{{40}})", image_tag or ""
    )
    if (
        not match
        or not isinstance(username, str)
        or not re.fullmatch(r"[A-Za-z0-9-]{1,256}", username)
        or not isinstance(password, str)
        or not password
    ):
        raise RegistryCheckError("registry_check_configuration_invalid")

    tag = "sha-" + match.group(1)
    manifest_url = f"https://{REGISTRY}/v2/{repository}/manifests/{tag}"
    manifest_headers = {"Accept": MANIFEST_ACCEPT}
    status, headers, body = _response(opener, Request(manifest_url, headers=manifest_headers))
    if status == 200:
        return True
    if status == 404:
        not_found_code = _not_found_code(body)
        if not_found_code not in {"MANIFEST_UNKNOWN", "NAME_UNKNOWN"}:
            raise RegistryCheckError("registry_manifest_check_failed")
        # Authenticate before treating any 404 as proof that the tag is absent.
        scope = f"repository:{repository}:pull"
    elif status == 401:
        scope = _challenge_parameters(headers.get("WWW-Authenticate"), repository)
    else:
        raise RegistryCheckError("registry_manifest_check_failed")

    token = _access_token(opener, username, password, scope)

    manifest_headers["Authorization"] = f"Bearer {token}"
    status, _, body = _response(opener, Request(manifest_url, headers=manifest_headers))
    if status == 200:
        return True
    if status == 404 and _not_found_code(body) in {"MANIFEST_UNKNOWN", "NAME_UNKNOWN"}:
        return False
    if status == 401 or status == 403:
        raise RegistryCheckError("registry_auth_error")
    raise RegistryCheckError("registry_manifest_check_failed")


def main():
    try:
        exists = tag_exists(
            os.environ.get("IMAGE_TAG", ""),
            os.environ.get("GHCR_USERNAME", ""),
            os.environ.get("GHCR_TOKEN", ""),
            repository=os.environ.get("GHCR_REPOSITORY", DEFAULT_REPOSITORY),
        )
        if exists:
            print("::error::The immutable image SHA tag already exists", file=sys.stderr)
            return 1
        print("Image SHA tag is confirmed absent")
        return 0
    except RegistryCheckError as error:
        print(f"::error::Could not verify image SHA tag ({error})", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
