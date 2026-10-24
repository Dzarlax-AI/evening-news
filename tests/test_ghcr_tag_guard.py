import io
import json
from pathlib import Path
import sys
import unittest
from email.message import Message
from urllib.error import HTTPError, URLError
from urllib.request import Request

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from scripts.check_ghcr_tag_absent import RegistryCheckError, _NoRedirect, tag_exists


TAG = "ghcr.io/dzarlax-ai/rss-scheduler:sha-0123456789abcdef0123456789abcdef01234567"
WORKER_TAG = "ghcr.io/dzarlax-ai/rss-job-worker:sha-0123456789abcdef0123456789abcdef01234567"
INGESTION_TAG = "ghcr.io/dzarlax-ai/rss-article-ingestion:sha-0123456789abcdef0123456789abcdef01234567"
CHALLENGE = (
    'Bearer realm="https://ghcr.io/token",service="ghcr.io",'
    'scope="repository:dzarlax-ai/rss-scheduler:pull"'
)
WORKER_CHALLENGE = (
    'Bearer realm="https://ghcr.io/token",service="ghcr.io",'
    'scope="repository:dzarlax-ai/rss-job-worker:pull"'
)
INGESTION_CHALLENGE = (
    'Bearer realm="https://ghcr.io/token",service="ghcr.io",'
    'scope="repository:dzarlax-ai/rss-article-ingestion:pull"'
)


class Response:
    def __init__(self, status, body=b"", headers=None):
        self.status = status
        self.headers = headers or Message()
        self.body = body

    def read(self, limit=-1):
        return self.body[:limit]

    def close(self):
        pass


def unauthorized(request):
    headers = Message()
    headers["WWW-Authenticate"] = CHALLENGE
    raise HTTPError(request.full_url, 401, "unauthorized", headers, io.BytesIO(b"{}"))


class RegistryTagGuardTests(unittest.TestCase):
    def check(self, opener):
        return tag_exists(TAG, "fixture-user", "fixture-token", opener=opener)

    def test_existing_tag_is_reported_present(self):
        requests = []

        def opener(request, timeout):
            requests.append(request)
            if len(requests) == 1:
                unauthorized(request)
            if len(requests) == 2:
                self.assertEqual(request.get_header("Authorization"), "Basic Zml4dHVyZS11c2VyOmZpeHR1cmUtdG9rZW4=")
                return Response(200, b'{"token":"test-bearer"}')
            self.assertEqual(request.get_header("Authorization"), "Bearer test-bearer")
            return Response(200, b"{}")

        self.assertTrue(self.check(opener))

    def test_explicit_manifest_unknown_means_absent(self):
        requests = []

        def opener(request, timeout):
            requests.append(request)
            if len(requests) == 1:
                unauthorized(request)
            if len(requests) == 2:
                return Response(200, b'{"access_token":"test-bearer"}')
            return Response(404, b'{"errors":[{"code":"MANIFEST_UNKNOWN"}]}')

        self.assertFalse(self.check(opener))

    def test_unauthenticated_manifest_unknown_is_rechecked_before_absence(self):
        calls = 0

        def opener(request, timeout):
            nonlocal calls
            calls += 1
            if calls == 1:
                raise HTTPError(
                    request.full_url,
                    404,
                    "not found",
                    Message(),
                    io.BytesIO(b'{"errors":[{"code":"MANIFEST_UNKNOWN"}]}'),
                )
            if calls == 2:
                return Response(200, b'{"access_token":"test-bearer"}')
            self.assertEqual(request.get_header("Authorization"), "Bearer test-bearer")
            return Response(200, b"existing manifest")

        self.assertTrue(self.check(opener))
        self.assertEqual(calls, 3)

    def test_explicit_unknown_package_means_first_publication_is_absent(self):
        calls = 0

        def opener(request, timeout):
            nonlocal calls
            calls += 1
            if calls == 1:
                headers = Message()
                headers["WWW-Authenticate"] = CHALLENGE
                raise HTTPError(
                    request.full_url,
                    404,
                    "not found",
                    headers,
                    io.BytesIO(b'{"errors":[{"code":"NAME_UNKNOWN"}]}'),
                )
            if calls == 2:
                self.assertTrue(request.full_url.startswith("https://ghcr.io/token?"))
                return Response(200, b'{"token":"test-bearer"}')
            self.assertEqual(request.get_header("Authorization"), "Bearer test-bearer")
            return Response(404, b'{"errors":[{"code":"NAME_UNKNOWN"}]}')

        self.assertFalse(self.check(opener))

    def test_unauthenticated_unknown_package_is_rechecked_before_absence(self):
        calls = 0

        def opener(request, timeout):
            nonlocal calls
            calls += 1
            if calls == 1:
                headers = Message()
                headers["WWW-Authenticate"] = CHALLENGE
                raise HTTPError(
                    request.full_url,
                    404,
                    "not found",
                    headers,
                    io.BytesIO(b'{"errors":[{"code":"NAME_UNKNOWN"}]}'),
                )
            if calls == 2:
                return Response(200, b'{"token":"test-bearer"}')
            self.assertEqual(request.get_header("Authorization"), "Bearer test-bearer")
            return Response(200, b"existing manifest")

        self.assertTrue(self.check(opener))
        self.assertEqual(calls, 3)

    def test_transport_error_fails_closed_without_details(self):
        def opener(_request, timeout):
            raise URLError("fixture-only sensitive text")

        with self.assertRaises(RegistryCheckError) as caught:
            self.check(opener)
        self.assertEqual(str(caught.exception), "registry_transport_error")
        self.assertNotIn("sensitive", str(caught.exception))

    def test_auth_error_fails_closed(self):
        def opener(request, timeout):
            if request.full_url.startswith("https://ghcr.io/token?"):
                raise HTTPError(request.full_url, 403, "forbidden", Message(), io.BytesIO(b"{}"))
            unauthorized(request)

        with self.assertRaises(RegistryCheckError) as caught:
            self.check(opener)
        self.assertEqual(str(caught.exception), "registry_auth_error")

    def test_redirect_fails_closed_without_following_registry_response(self):
        calls = 0

        def opener(request, timeout):
            nonlocal calls
            calls += 1
            headers = Message()
            headers["Location"] = "https://attacker.invalid/collect"
            raise HTTPError(request.full_url, 302, "redirect", headers, io.BytesIO(b""))

        with self.assertRaises(RegistryCheckError) as caught:
            self.check(opener)
        self.assertEqual(str(caught.exception), "registry_manifest_check_failed")
        self.assertEqual(calls, 1)

    def test_registry_http_handler_disables_redirects(self):
        request = Request("https://ghcr.io/token")
        self.assertIsNone(_NoRedirect().redirect_request(
            request,
            None,
            302,
            "found",
            {"Location": "https://attacker.invalid/collect"},
            "https://attacker.invalid/collect",
        ))

    def test_go_worker_sha_tag_uses_its_own_repository_and_scope(self):
        calls = 0

        def opener(request, timeout):
            nonlocal calls
            calls += 1
            if calls == 1:
                headers = Message()
                headers["WWW-Authenticate"] = WORKER_CHALLENGE
                raise HTTPError(
                    request.full_url,
                    401,
                    "unauthorized",
                    headers,
                    io.BytesIO(b"{}"),
                )
            if calls == 2:
                self.assertIn("repository%3Adzarlax-ai%2Frss-job-worker%3Apull", request.full_url)
                return Response(200, b'{"token":"worker-test-token"}')
            self.assertIn("/v2/dzarlax-ai/rss-job-worker/manifests/sha-", request.full_url)
            self.assertEqual(request.get_header("Authorization"), "Bearer worker-test-token")
            return Response(404, b'{"errors":[{"code":"MANIFEST_UNKNOWN"}]}')

        self.assertFalse(tag_exists(
            WORKER_TAG, "fixture-user", "fixture-token", opener=opener,
            repository="dzarlax-ai/rss-job-worker",
        ))

    def test_go_article_ingestion_sha_tag_uses_its_own_repository_and_scope(self):
        calls = 0

        def opener(request, timeout):
            nonlocal calls
            calls += 1
            if calls == 1:
                headers = Message()
                headers["WWW-Authenticate"] = INGESTION_CHALLENGE
                raise HTTPError(
                    request.full_url,
                    401,
                    "unauthorized",
                    headers,
                    io.BytesIO(b"{}"),
                )
            if calls == 2:
                self.assertIn("repository%3Adzarlax-ai%2Frss-article-ingestion%3Apull", request.full_url)
                return Response(200, b'{"token":"ingestion-test-token"}')
            self.assertIn("/v2/dzarlax-ai/rss-article-ingestion/manifests/sha-", request.full_url)
            self.assertEqual(request.get_header("Authorization"), "Bearer ingestion-test-token")
            return Response(404, b'{"errors":[{"code":"MANIFEST_UNKNOWN"}]}')

        self.assertFalse(tag_exists(
            INGESTION_TAG, "fixture-user", "fixture-token", opener=opener,
            repository="dzarlax-ai/rss-article-ingestion",
        ))

    def test_registry_guard_rejects_unapproved_repository(self):
        with self.assertRaises(RegistryCheckError) as caught:
            tag_exists(TAG, "fixture-user", "fixture-token", repository="attacker/package")
        self.assertEqual(str(caught.exception), "registry_check_configuration_invalid")

    def test_non_manifest_404_fails_closed(self):
        calls = 0

        def final_not_found(request, timeout):
            nonlocal calls
            calls += 1
            if calls == 1:
                unauthorized(request)
            if calls == 2:
                return Response(200, b'{"token":"test-bearer"}')
            return Response(404, json.dumps({"errors": [{"code": "DENIED"}]}).encode())

        with self.assertRaises(RegistryCheckError):
            self.check(final_not_found)


if __name__ == "__main__":
    unittest.main()
