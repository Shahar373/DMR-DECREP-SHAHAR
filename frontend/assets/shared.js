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

  /* Day picker: ◀ [select: Live/today + recorded days] ▶, fed by
   * /api/days. State lives in the URL (?day=YYYY-MM-DD) so day views
   * are linkable. `onChange(day)` gets '' for live/today. */
  function createDayPicker(container, onChange) {
    var sel = document.createElement('select');
    sel.className = 'day-picker';
    sel.title = 'Pick a recorded day (or live)';
    var prev = document.createElement('button');
    var next = document.createElement('button');
    prev.type = next.type = 'button';
    prev.textContent = '◀';
    next.textContent = '▶';
    prev.title = 'Previous day';
    next.title = 'Next day';
    prev.className = next.className = 'day-step';
    container.appendChild(prev);
    container.appendChild(sel);
    container.appendChild(next);

    function current() { return sel.value; }

    function step(delta) {
      var opts = Array.prototype.map.call(sel.options, function (o) { return o.value; });
      var i = opts.indexOf(sel.value) + delta;
      if (i < 0 || i >= opts.length) return;
      sel.value = opts[i];
      apply();
    }
    prev.addEventListener('click', function () { step(1); });   // options newest→oldest
    next.addEventListener('click', function () { step(-1); });

    function apply() {
      var day = current();
      var url = new URL(location.href);
      if (day) url.searchParams.set('day', day);
      else url.searchParams.delete('day');
      history.replaceState(null, '', url);
      onChange(day);
    }
    sel.addEventListener('change', apply);

    fetch('/api/days').then(function (r) { return r.json(); })
      .then(function (d) {
        var days = (d.days || []).map(function (x) { return x.day; })
          .filter(function (x) { return x && x !== 'unknown'; });
        days.sort().reverse();  // newest first
        var today = new Date().toISOString().slice(0, 10);
        var html = '<option value="">Today — live</option>';
        days.forEach(function (day) {
          if (day === today) return;  // covered by the live option
          html += '<option value="' + day + '">' + day + '</option>';
        });
        sel.innerHTML = html;
        var want = new URL(location.href).searchParams.get('day') || '';
        if (want && days.indexOf(want) !== -1 && want !== today) {
          sel.value = want;
          onChange(want);
        }
      })
      .catch(function () {});
    return { current: current };
  }

  window.DMRShared = { createDayPicker: createDayPicker };

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
