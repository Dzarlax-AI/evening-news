"""Exercise public feed card rendering with attacker-controlled persisted fields."""

import json
import shutil
import subprocess
from pathlib import Path

import pytest
from bs4 import BeautifulSoup


TEMPLATE = Path(__file__).parents[1] / "web/templates/public/_feed_core.html"


@pytest.mark.skipif(shutil.which("node") is None, reason="Node.js is needed to execute feed rendering")
def test_feed_card_escapes_attributes_and_omits_unsafe_urls():
    template = TEMPLATE.read_text()
    script = template.split("<script>", 1)[1].split("</script>", 1)[0]
    marker = "  // Load category configuration from API (with caching)"
    assert marker in script
    script = script.replace(marker, "  globalThis.__renderFeedCard = containerHtmlFor;\n" + marker, 1)
    node_script = r"""
const vm = require('node:vm');
global.window = global;
global.location = {href: 'https://news.example.test/section/'};
global.document = {
  getElementById: () => null,
  createElement: () => ({style: {}, parentElement: null}),
  addEventListener: () => {},
};
global.IntersectionObserver = undefined;
global.__feedScript = %s;
vm.runInThisContext(__feedScript);
const cards = [
  __renderFeedCard({
    id: 1,
    title: 'Headline" onerror="alert(1)',
    category: 'news" onclick="alert(2)',
    url: 'javascript:alert(3)',
    primary_image: '../cover.jpg',
    published_at: '2026-10-23T12:00:00Z',
  }),
  __renderFeedCard({
    id: 2,
    title: 'Unsafe image',
    url: 'https://example.test/story" onclick="alert(4)',
    primary_image: 'https://cdn.example.test/image.jpg" onerror="alert(5)',
    published_at: '2026-10-23T12:00:00Z',
  }),
  __renderFeedCard({
    id: 3,
    title: 'Legacy relative links',
    url: '../story/3',
    primary_image: '/cover/3.jpg',
    published_at: '2026-10-23T12:00:00Z',
  }),
  __renderFeedCard({
    id: 4,
    title: 'Credential URL',
    url: 'https://user:secret@example.test/story',
    primary_image: 'https://user:secret@cdn.example.test/image.jpg',
    published_at: '2026-10-23T12:00:00Z',
  }),
];
process.stdout.write(JSON.stringify(cards));
""" % json.dumps(script)
    rendered = subprocess.run(
        [shutil.which("node"), "-e", node_script],
        check=True,
        capture_output=True,
        text=True,
        timeout=10,
    )
    cards = json.loads(rendered.stdout)

    malicious = BeautifulSoup(cards[0], "html.parser").article
    assert malicious is not None
    assert malicious.get("data-category") == 'news" onclick="alert(2)'
    assert "onerror" not in malicious.attrs
    image = malicious.img
    assert image is not None
    assert image["alt"] == 'Headline" onerror="alert(1)'
    assert image["src"] == "https://news.example.test/cover.jpg"
    assert not malicious.select("a.btn-original")  # javascript: URL was omitted

    unsafe = BeautifulSoup(cards[1], "html.parser").article
    assert unsafe is not None
    assert unsafe.img is None
    assert not unsafe.select("a.btn-original")
    assert unsafe["onclick"] == "openArticleModal(2)"

    relative = BeautifulSoup(cards[2], "html.parser").article
    assert relative is not None
    assert relative.select_one("a.btn-original")["href"] == "https://news.example.test/story/3"
    assert relative.img["src"] == "https://news.example.test/cover/3.jpg"

    credentials = BeautifulSoup(cards[3], "html.parser").article
    assert credentials is not None
    assert credentials.img is None
    assert not credentials.select("a.btn-original")
