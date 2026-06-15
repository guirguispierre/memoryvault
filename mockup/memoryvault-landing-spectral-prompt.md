# Task: Spectral as the site-wide default, and a real marketing landing page

Addition to PR #39 (or a fresh branch off it if you prefer — say so). Two things:
**(A)** make the "paper" light theme with **Spectral** the default appearance
across every page; **(B)** replace the bare root page with a real marketing
landing page that explains the product, in the paper design, with a light/dark
toggle.

Reference mockups ship with this prompt:
- `mockup/paper-spectral.html` + `mockup/paper-spectral-login.png` — the app
  viewer/login in the final Spectral paper look (the design system to reuse).
- `mockup/landing.html` + section screenshots `landing-top.png`,
  `landing-mid.png`, `landing-pricing.png` — the target landing page.
Read them before coding and match them closely.

Standing rules: no behavior changes to auth/OAuth logic, isolation, or
`brain_id` scoping; no new runtime deps; no build step (pages ship as strings
from the Worker); `npx tsc --noEmit` clean; `npm test` + live isolation suite
green (20/20); no `any`, no dead/commented-out CSS, no emoji in chrome, no raw
hex outside the theme token blocks; comments only where non-obvious; human-voice
commits and PR-description updates per the established PR #39 standard.

## Part A — Spectral "paper" as the default everywhere

The paper theme tokens and the Spectral/Inter/JetBrains Mono type roles are
defined in `mockup/paper-spectral.html`. (Font: `Spectral:ital,wght@0,400;0,500;
0,600;1,400`, set as the `--doc` content face; Inter for UI chrome; JetBrains
Mono for data.)

1. If the `paper` theme already exists from the prior theme task, switch its
   content font from whatever serif it used to **Spectral** and confirm the
   tokens match the Spectral mockup. If it doesn't exist yet, add it to
   `themeStyles` in `src/viewer/themes.ts` (light theme) plus a dark counterpart,
   using the exact paper tokens, mapped onto the existing component variable
   names (ground/cream/rule/butter→accent/sage→good, etc. — define the full set
   every other theme defines).
2. Make `paper` the **default light theme** and pair it so `theme_mode: 'auto'`
   resolves to the clean-dark default at night and paper by day. Set this in
   `defaultSettings` (client) and ensure the server-rendered pages' bootstrap
   resolver defaults to the same when no setting exists. Existing users keep
   their saved theme; only the absence of a setting falls through to paper.
3. Register `paper` in every place themes are registered (client `validThemes`,
   server `sanitizeViewerSettings` whitelist, theme picker, bootstrap resolver) —
   it must be selectable and must theme the viewer, login, landing, /mcp,
   guides, and OAuth pages.
4. Apply the centered reading column (from the paper design) across the app
   themes; the graph view stays full-width. (If the prior task already did this,
   just confirm it holds with Spectral.)

## Part B — the marketing landing page at `/`

Today `/` (`rootLandingHtml` in `src/routes.ts`) is a bare endpoint list. Replace
it with a real landing page modeled on `mockup/landing.html`. It is a public,
unauthenticated page. Build these sections, in order:

1. **Sticky nav** — wordmark `memoryvault.md`, links (Features, How it works,
   Pricing, FAQ), a **light/dark theme toggle**, Docs, and a primary "Get
   started" button.
2. **Hero** — eyebrow ("open source · self-hosted · graph-aware"), headline
   "The memory layer your agents *actually own*" (Spectral, italic accent on the
   last words), one-paragraph subhead, dual CTA ("Deploy your own — free" +
   "View on GitHub"), and a mono sub-line ("MIT licensed · graph included ·
   deploy to Cloudflare in ~5 min").
3. **Product shot** — embed a real screenshot of the paper viewer (use the
   generated viewer screenshot; wire the build to drop the current viewer shot
   into the page's assets, or inline an SVG/HTML facsimile if serving a binary
   asset from the Worker is awkward — pick the simpler robust option and note
   which).
4. **Feature section 1 — "A graph of memory, nothing paywalled"** with a small
   ledger-style panel (key → value, verified tags).
5. **Feature section 2 — "Self-hosted. You own the data."** with a config-snippet
   panel (the `[[vectorize]]` / `[[d1_databases]]` lines + "memories never leave
   your Cloudflare account").
6. **How it works** — three steps: Deploy your vault / Connect your agents /
   Watch it strengthen.
7. **Agents strip** — "Works with your stack": Claude, Claude Code, Codex, any
   MCP client, REST API.
8. **Pricing** — two plans: **Self-host $0** (full source, graph, your data,
   community support) and **Hosted $12/mo** (managed sync, backups, priority
   support), hosted plan visually featured.
9. **FAQ** — the four Q&As from the mockup (open source? data location? agents?
   how is it different — the last one lands the isolation-test point).
10. **Final CTA** + **footer** (© MemoryVault · MIT licensed · "all systems
    operational").

Content/voice rules:
- Lead order is deliberate: graph + nothing-paywalled first, self-hosted second,
  any-agent third, security as the closing FAQ punch. Keep that priority.
- This is **our** page — do not lift copy or layout specifics from any
  competitor. The mockup copy is the source of truth; refine for accuracy but
  keep it factual, no marketing fluff or superlatives.
- Pricing: the $12/mo hosted tier is a stated intent, not a live product. If
  there's no billing yet, the hosted CTA should go to a waitlist/contact or be
  labeled "coming soon" rather than implying instant signup. Use your judgment
  and note what you wired it to.
- Every CTA links somewhere real: GitHub repo, the `/view` app (Get started /
  Deploy), and docs. Don't leave dead `#` links except in-page anchors.

Technical:
- The landing page shares the paper design tokens and the same theme bootstrap
  as the rest of the site, so the nav toggle and the saved/`auto` theme both
  work; light/dark must both look right (provide the dark token values for the
  landing surfaces too).
- Server-rendered, hand-written CSS, no framework. Responsive: the nav collapses
  sensibly on mobile, the feature grids stack to one column, the hero headline
  scales down, the pricing cards stack.
- Keep the existing endpoint/guide information reachable (e.g. under Docs or a
  `/endpoints` link) — don't simply delete the developer-facing reference that
  used to live at `/`.

## Verification (paste in report)

```bash
npx tsc --noEmit
npm test
BASE_URL=http://127.0.0.1:8787 npm run test:isolation     # 20/20
grep -rn ": any\b\| as any\b" src/                         # empty
```

Extend the screenshot script:
- the **landing page** full-page in **light (paper)** at 1280 wide,
- the landing page full-page in **dark**,
- the landing page at **390px** (mobile reflow — nav, hero, grids, pricing),
- the **app viewer** in paper to confirm the shared look,
- one **server page** (OAuth or /mcp) in paper to confirm site-wide theming.
Save under `mockup/shots/`, reference them, self-critique vs the mockups
(Spectral in headers, accent only on CTAs/links/active states, AA contrast in
both light and dark, no overlap, mobile reflow clean).

## Commits / PR description (human voice)

- Coherent commits on the branch, e.g.
  `theme: default to the paper (Spectral) light theme site-wide`,
  `landing: build the marketing homepage at /`.
  Imperative, lower-case, <~70 chars, no trailing period, no tool trailers/emoji;
  short bodies on the why.
- Update the PR description: default appearance is now the paper/Spectral light
  theme (auto pairs it with clean-dark), and `/` is a real marketing landing
  page with a light/dark toggle; developer endpoint reference moved under Docs.

## Report

1. Where the paper/Spectral tokens live and the default-resolution logic.
2. How the product-shot asset is served (screenshot file vs inline facsimile).
3. What each CTA links to, and how the hosted-tier CTA is handled given no
   billing yet.
4. Confirmation the old endpoint reference is still reachable.
5. The screenshots listed above (light, dark, mobile).
6. `tsc` + test + isolation + grep output.
7. Proposed commit subjects/bodies and PR-description addition for review before
   pushing.
8. Anything intentionally deferred and why.

Stop and flag rather than proceed if any item would require server, auth, or
isolation changes beyond the additive theme-whitelist entry and the new public
landing route.
