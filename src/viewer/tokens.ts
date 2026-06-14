// Single source of truth for the vanilla design system shared by every page
// the worker serves: the viewer at /view plus the landing, /mcp, endpoint
// guide, and OAuth screens. Pages import these strings rather than pasting
// hex values, so a palette change here flows everywhere at once.

export const FONT_LINK_TAGS = `<link rel="preconnect" href="https://fonts.googleapis.com">
<link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
<link href="https://fonts.googleapis.com/css2?family=Fraunces:ital,opsz,wght@0,9..144,420;0,9..144,560;0,9..144,640;1,9..144,420;1,9..144,560&family=Schibsted+Grotesk:wght@400;500;600&family=IBM+Plex+Mono:wght@400;500&display=swap" rel="stylesheet">`;

// The default (dark) vanilla tokens. The viewer's theme variants in themes.ts
// build on top of this block; the server-rendered pages emit it verbatim.
export const vanillaTokensCss = `  :root {
    --ground: #181511;
    --ground-2: #201C16;
    --ground-3: #26211A;
    --rule: #332C22;
    --rule-soft: #2A2419;
    --rule-bright: #453B2C;
    --cream: #F0E7D5;
    --cream-dim: #B5AB97;
    --cream-faint: #7E7666;
    --butter: #E3C98F;
    --butter-deep: #A98F5C;
    --latte: #8C8170;
    --sage: #9DB39A;
    --clay: #C9826E;
    --on-butter: #241C0D;
    --butter-glow: rgba(227, 201, 143, 0.07);
    --disp: 'Fraunces', Georgia, serif;
    --body: 'Schibsted Grotesk', system-ui, sans-serif;
    --mono: 'IBM Plex Mono', ui-monospace, monospace;
    --overlay-bg: rgba(16, 13, 10, 0.78);
    --panel-bg: #201C16;
    --panel-shadow: rgba(0, 0, 0, 0.5);
    --surface: rgba(32, 28, 22, 0.6);
    --surface-raised: #241F18;
    --toast-bg: #241F18;
    --card-glow: rgba(0, 0, 0, 0.35);

    /* Legacy aliases: graph client reads these via getComputedStyle and
       relation styling keys off them, so they track the vanilla tokens. */
    --bg: var(--ground);
    --bg2: var(--ground-2);
    --bg3: var(--ground-3);
    --border: var(--rule);
    --border-bright: var(--rule-bright);
    --amber: var(--butter);
    --amber-dim: var(--butter-deep);
    --amber-glow: var(--butter-glow);
    --teal: var(--sage);
    --red: var(--clay);
    --text: var(--cream-dim);
    --text-dim: var(--cream-faint);
    --text-bright: var(--cream);
    --journal: var(--latte);
    --success: var(--sage);
    --info: var(--latte);
    --causes: var(--butter-deep);
  }
`;

// Shared chrome for the document pages (landing, /mcp, endpoint guide). Colour
// and font only ever come from the tokens above — page-specific layout (tables,
// metric tiles, grids) is added by each page on top of this.
export const pageChromeCss = `  * { box-sizing: border-box; }
  body {
    margin: 0;
    font-family: var(--body);
    color: var(--cream-dim);
    -webkit-font-smoothing: antialiased;
    background:
      radial-gradient(1000px 460px at 50% -12%, var(--butter-glow), transparent 62%),
      var(--ground);
    min-height: 100vh;
  }
  a { color: var(--butter); }
  .wrap { max-width: 1100px; margin: 0 auto; padding: 2.4rem 1.3rem 3rem; }
  .pill {
    display: inline-flex;
    align-items: center;
    border: 1px solid var(--rule);
    border-radius: 7px;
    background: var(--butter-glow);
    color: var(--butter);
    font-family: var(--mono);
    font-size: 10px;
    letter-spacing: 0.12em;
    text-transform: uppercase;
    padding: 0.32rem 0.6rem;
    margin-bottom: 1.1rem;
  }
  .title {
    margin: 0;
    font-family: var(--disp);
    font-weight: 560;
    font-size: clamp(1.7rem, 3vw, 2.7rem);
    letter-spacing: 0.005em;
    line-height: 1.06;
    color: var(--cream);
  }
  .title span { font-style: italic; color: var(--butter); }
  .sub {
    margin: 0.55rem 0 1.5rem;
    color: var(--cream-faint);
    font-family: var(--mono);
    letter-spacing: 0.1em;
    text-transform: uppercase;
    font-size: 0.7rem;
  }
  .card {
    border: 1px solid var(--rule);
    border-radius: 12px;
    background: var(--surface-raised);
    padding: 1.2rem 1.2rem 1.1rem;
  }
  .card h2 {
    margin: 0 0 0.7rem;
    color: var(--butter);
    font-family: var(--disp);
    font-weight: 560;
    font-size: 0.95rem;
    letter-spacing: 0.01em;
  }
  p, li { margin: 0; line-height: 1.6; font-size: 0.9rem; color: var(--cream-dim); }
  ul, ol { margin: 0; padding-left: 1.1rem; display: grid; gap: 0.5rem; }
  code { font-family: var(--mono); color: var(--butter); font-size: 0.82rem; }
  .actions { margin-top: 1rem; display: flex; flex-wrap: wrap; gap: 0.55rem; }
  .btn {
    border: 1px solid var(--rule);
    border-radius: 8px;
    color: var(--cream-dim);
    text-decoration: none;
    font-family: var(--mono);
    font-size: 0.68rem;
    letter-spacing: 0.08em;
    text-transform: uppercase;
    padding: 0.5rem 0.72rem;
    display: inline-block;
    transition: border-color 0.15s, color 0.15s;
  }
  .btn:hover { border-color: var(--butter-deep); color: var(--butter); }
  .btn.primary { border-color: var(--butter); background: var(--butter); color: var(--on-butter); }
  .btn.primary:hover { filter: brightness(1.05); color: var(--on-butter); }
  .endpoint {
    color: var(--butter);
    font-family: var(--mono);
    text-decoration: none;
    overflow-wrap: anywhere;
  }
  .endpoint:hover { color: var(--cream); }
  :focus-visible { outline: 2px solid var(--butter); outline-offset: 2px; }
`;
