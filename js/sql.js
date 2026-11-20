// A tiny SQL highlighter. Each source line becomes a <span class="ln" data-line="n">
// so the player can light up the lines that belong to the active operator.
'use strict';

const SQL = (() => {
  const KEYWORDS = new Set(`
    select from where insert into group by order partition over as on and or not null is
    join left right inner outer full lateral cross union all distinct having limit
    create table with primary key enforced watermark for interval second seconds minute minutes
    hour hours day row_number rows range between case when then else end cast
    system_time of table descriptor tumble hop cumulate session match_recognize measures
    define pattern within one per match after skip past last first asc desc true false
    explain changelog_mode
  `.trim().split(/\s+/));

  const TYPES = new Set(['bigint', 'int', 'integer', 'string', 'varchar', 'decimal',
    'timestamp', 'timestamp_ltz', 'boolean', 'double', 'float', 'date', 'time']);

  const TOKEN = /(--[^\n]*)|('(?:[^']|'')*')|(\b\d+(?:\.\d+)?\b)|([A-Za-z_][A-Za-z0-9_$]*)|(\s+)|(.)/g;

  function esc(s) {
    return s.replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
  }

  function highlightLine(line) {
    let out = '';
    for (const m of line.matchAll(TOKEN)) {
      const [tok, comment, str, num, word] = m;
      if (comment) out += `<span class="t-com">${esc(tok)}</span>`;
      else if (str) out += `<span class="t-str">${esc(tok)}</span>`;
      else if (num) out += `<span class="t-num">${tok}</span>`;
      else if (word && KEYWORDS.has(word.toLowerCase())) out += `<span class="t-kw">${tok}</span>`;
      else if (word && TYPES.has(word.toLowerCase())) out += `<span class="t-type">${tok}</span>`;
      else if (word && /^[A-Z_]+$/.test(word) && word.length > 2) out += `<span class="t-fn">${tok}</span>`;
      else out += esc(tok);
    }
    return out;
  }

  return {
    // Returns a <pre><code> element with one span per line.
    render(sql, label) {
      const code = U.el('code', { class: 'sql' });
      sql.trim().split('\n').forEach((line, i) => {
        const span = U.el('span', { class: 'ln', 'data-line': i + 1 });
        span.innerHTML = highlightLine(line) || ' ';
        code.append(span, '\n');
      });
      return U.el('pre', { class: 'code', 'aria-label': label }, code);
    },
    mark(pre, lines) {
      const want = new Set(lines || []);
      pre.querySelectorAll('.ln').forEach((ln) => {
        ln.classList.toggle('hot', want.has(Number(ln.dataset.line)));
      });
    },
  };
})();
