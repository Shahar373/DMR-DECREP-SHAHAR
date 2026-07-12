"""Phase-4 tests: every page serves 200 and wires the shared assets."""
from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

from backend import server as srv

PAGES = ["/", "/debrief", "/stats", "/network", "/alerts"]


@pytest.fixture
def client():
    return TestClient(srv.app)


@pytest.mark.parametrize("path", PAGES)
def test_page_serves_and_references_shared_assets(client, path):
    r = client.get(path)
    assert r.status_code == 200
    assert "/assets/style.css" in r.text
    assert "/assets/shared.js" in r.text
    # Shared injected nav placeholder present on every page.
    assert "data-shared-nav" in r.text
    # Viewport meta — required for any mobile layout to apply.
    assert 'name="viewport"' in r.text


def test_style_css_served_with_css_mime(client):
    r = client.get("/assets/style.css")
    assert r.status_code == 200
    assert "text/css" in r.headers["content-type"]
    # Design tokens present.
    assert "--bg:" in r.text


def test_shared_js_served(client):
    r = client.get("/assets/shared.js")
    assert r.status_code == 200
    assert "initNav" in r.text


def test_no_page_hardcodes_its_own_nav_links(client):
    """The nav is injected by shared.js — a page carrying its own literal
    nav links would drift from the shared one."""
    for path in PAGES:
        html = client.get(path).text
        assert '<a href="/debrief">Debrief</a>' not in html, path


def test_index_guards_missing_leaflet():
    """A CDN outage (offline Pi, blocked network — a real deployment
    scenario for this project) must not crash the whole dashboard script.
    Regression test for the pre-9.0 bug where `const map = L.map(...)`
    was the first line of the inline script with no guard: if Leaflet
    failed to load, every feature below it (radios table, calls, debrief,
    the SDR panel) silently died along with the map."""
    client = TestClient(srv.app)
    html = client.get("/").text
    assert "HAS_LEAFLET" in html
    assert "typeof L !== 'undefined'" in html
    # The very first executable line must not unconditionally dereference L.
    assert "const map = L.map(" not in html
