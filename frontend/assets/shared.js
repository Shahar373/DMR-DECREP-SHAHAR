/* DMR Cap+ Monitor — shared header/nav + version tag (v0.21.0).
 *
 * Every page carries an empty <nav class="nav" data-shared-nav></nav>;
 * this script fills it with the site links (active one derived from the
 * pathname), adds a hamburger toggle for narrow screens, and fetches
 * /api/version once into #m-version and/or #version-tag. Injected in JS
 * (rather than server-side templating) so pages stay standalone and no
 * backend template dependency is needed — every page already requires
 * JS to function.
 */
(function () {
  'use strict';

  var LINKS = [
    ['/', 'Live'],
    ['/debrief', 'Debrief'],
    ['/stats', 'Stats'],
    ['/network', 'Network'],
    ['/alerts', 'Alerts'],
  ];

  function initNav() {
    var nav = document.querySelector('[data-shared-nav]');
    if (!nav) return;
    var here = location.pathname.replace(/\/+$/, '') || '/';
    nav.innerHTML = LINKS.map(function (l) {
      var active = (l[0] === here) ? ' class="active"' : '';
      return '<a href="' + l[0] + '"' + active + '>' + l[1] + '</a>';
    }).join('');

    // Hamburger for <640px (CSS hides the toggle on wide screens).
    var btn = document.createElement('button');
    btn.type = 'button';
    btn.className = 'nav-toggle';
    btn.setAttribute('aria-label', 'Menu');
    btn.setAttribute('aria-expanded', 'false');
    btn.textContent = '☰';
    btn.addEventListener('click', function () {
      var open = nav.classList.toggle('open');
      btn.setAttribute('aria-expanded', open ? 'true' : 'false');
    });
    nav.parentNode.insertBefore(btn, nav);
  }

  function initVersion() {
    fetch('/api/version').then(function (r) { return r.json(); })
      .then(function (v) {
        if (!v || !v.version) return;
        var m = document.getElementById('m-version');
        if (m) m.textContent = 'v' + v.version;
        var f = document.getElementById('version-tag');
        if (f) f.textContent = 'v' + v.version + ' (' + (v.build_date || '') + ')';
      })
      .catch(function () {});
  }

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', function () {
      initNav();
      initVersion();
    });
  } else {
    initNav();
    initVersion();
  }
})();
