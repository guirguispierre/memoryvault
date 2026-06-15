# Task: finalize the public site as the "Constellation" living-memory design

This is the consolidated, final spec for the MemoryVault public site, folding in
many rounds of real external feedback. It supersedes every earlier landing/theme
prompt. Build it on the `pr39` branch (or a branch off it).

A complete, working reference mockup ships with this prompt:
**`mockup/constellation-final.html`** — open it, read it, and match it closely.
It already contains the exact starfield canvas JS, the dead-zone math, the
gradient/wash treatment, the mobile nav, and all final copy. The deployed page
should look and behave like that file. Supporting screenshots:
`mockup/final-1-hero.png`, `final-2-feat.png`, `final-3-how.png`,
`final-mobile2.png`, `final-mobile2-menu.png`.
`mockup/paper-spectral.html` is the `/view` app (do NOT restyle it; referenced
only so you keep its theme system separate).

---

## 0. Anti-"AI-generated" rules (HARD CONSTRAINTS — read first)

Real users reviewed earlier versions and flagged specific "this looks
AI-generated" tells. These are now hard rules. Violating any of them fails the
task:

1. **No em-dashes anywhere in visible copy.** Not in headings, body, captions,
   buttons, FAQ, OAuth, docs — nowhere a person reads. Rewrite into periods,
   commas, or shorter sentences. Principle: period > comma > colon, basically
   never a dash. (The mockup copy has zero em-dashes; keep it that way. Grep your
   output: `grep -n "—" rendered-copy` must be empty for prose.)
2. **Monospace ONLY for genuine code.** Monospace (JetBrains Mono) is allowed
   solely for literal code, config, and terminal commands — the `wrangler.toml`
   block, install/deploy commands, ledger field-keys like `project.license`.
   **Never** use monospace for decoration: not eyebrows, nav, headings, body,
   taglines, or labels-that-aren't-code. Everything else is Inter (UI) or
   Spectral (display/serif).
3. **No decorative status-dot legends** in marketing sections. (An earlier
   active/settling/fading/reinforced legend under the hero was called "a very
   obvious AI-gen tell.") That meaning belongs in the app/docs, not the hero.
4. **No generic gradient-mesh / template hero.** The hero is the specific
   living-graph constellation described below, not a soft blurry gradient blob.
5. **Human copy.** Keep it blunt and specific. Use the mockup's copy verbatim
   unless a fix is needed; don't "enhance" it into marketing fluff.

---

## 1. Standing constraints (unchanged from PR #39)

- **No React, no framework, no build step, no bundler, no new dependencies.**
  Hand-written HTML/CSS + a small amount of vanilla JS, served as strings from
  the Worker. The starfield is vanilla `<canvas>`. (If anything here seems to
  need a build step, stop and flag — it doesn't.)
- **No changes to auth/OAuth logic, isolation, or `brain_id` scoping.** Public
  pages get the new *look* only. OAuth screens: theme only; forms, hidden fields,
  button name/values, redirects, and behavior stay byte-identical.
- `npx tsc --noEmit` clean; `npm test` + live isolation suite green (20/20);
  no `any`; no dead/commented-out code; comments only where non-obvious.
- Human-voice commits and PR description (imperative, lower-case subjects,
  <70 chars, no trailing period, no tool trailers, no emoji; short bodies on the
  why). No co-author/tool trailers.

---

## 2. The Constellation identity (shared theme tokens)

Define once in the shared theme module; every public page uses these:
```
--bg:#070810   --bg2:#0b0d18   --surface:#11131f
--ink:#f5f4ef  --dim:#9aa0b4   --faint:#565d72
--rule:rgba(255,255,255,0.08)
--accent:#8ab0ff (blue)  --good:#86e0b8 (green)  --warm:#ffcaa0  --violet:#b9a3ff
--doc:'Spectral'  --ui:'Inter'  --mono:'JetBrains Mono'
```
Deep-space background (fixed, `z-index:-1`): layered soft radial gradients — blue
wash top-center, faint green lower-right, faint violet lower-left — over `--bg`.
Exact stops are in the mockup.

**Scope of the identity vs the animation:**
- The **dark constellation theme** (tokens, type, nav/footer, gradients) applies
  to **all non-`/view` pages**: landing `/`, `/mcp`, endpoint/guide pages, docs,
  and the OAuth authorize/consent/login screens. The public site is one dark
  world. This supersedes the earlier "light paper public site" decision.
- The **full animated starfield runs ONLY on the landing hero.** Other pages
  (`/mcp`, docs, OAuth) use the same dark theme but **calm**: a static or very
  subtle low-density star layer, or just the gradient — never the full animation
  behind forms or long-form text. OAuth especially must keep its form perfectly
  legible.
- `/view` keeps its own existing theme system entirely. Don't touch it.

---

## 3. The landing hero (`/`) — living memory graph

Reproduce the canvas in `mockup/constellation-final.html` exactly. Key points
that make it look intentional (not a default particle library):

- **Each star is a radial gradient** (colored glow fading to transparent) with a
  **bright white core** on top. Lit-from-within, not a flat dot.
- **Glow is reduced and crisp** (an earlier version's soft bloom was flagged):
  small glow radius, low spread. Pinpricks of light, not fuzzy orbs.
- **Additive blending**: `ctx.globalCompositeOperation='lighter'` while drawing
  stars + links, reset to `'source-over'` after.
- **~12% "hero" stars** slightly larger/brighter; the rest small. Hierarchy.
- **Independent twinkle**: each star on its own sine cycle (random phase + rate).
- **Slow drift + cursor parallax** (per-star depth factor); wrap at edges.
- **Faint linking lines** between nearby stars (the "graph"), opacity by
  proximity.
- **Stars colored from the tier palette** (active/settling/fading/reinforced)
  for meaning, but **NO visible legend** in the hero.
- **TEXT DEAD-ZONE (important):** stars and links fade to nothing inside an
  elliptical region centered on the hero text, so nothing renders close behind
  the headline/subhead/CTAs. (See `deadZone()` in the mockup — it scales star and
  line opacity to 0 toward the text center.) Users explicitly asked for "no star
  near the text."

Hero content over the canvas (copy is final):
- eyebrow (Inter, not mono): **Open-source memory for AI agents**
- headline (Spectral): **Your agents forget everything. _Fix that._**
  (`Fix that.` italic + `--accent`); subtle text-shadow/glow for legibility.
- subhead: **MemoryVault gives any AI agent a memory it actually keeps. It learns
  what matters, drops what doesn't, and runs entirely on servers you own.**
- CTAs: solid **Deploy your own, free** (note: comma, no em-dash) + glass
  **View on GitHub**.
- No meta line, no legend.

### Performance + accessibility (MANDATORY for the animated canvas)
- **Cap star density by viewport width** (far fewer on mobile; never thousands).
- **Pause the rAF loop** when the hero is scrolled offscreen (IntersectionObserver)
  and when `document.hidden`.
- **Cap DPR at ~2**; keep it smooth (consider ~30–45fps throttle) on a mid laptop
  and a phone.
- **`prefers-reduced-motion`: render a single static starfield frame** — no
  drift/twinkle/parallax. (The mockup already branches on this.)
- Canvas is decorative: `aria-hidden`, `pointer-events:none`; all nav/CTAs stay
  keyboard-focusable and clickable above it.

---

## 4. Sections below the hero (one continuous dark space)

Keep content/order and match the mockup's styling. The **starfield ends below
the hero**; sections sit on the calm `--bg` with a single faint ambient wash
(`main::before` radial gradients) so the page reads as one continuous space.

- **No section divider lines** between sections (an earlier version had hairline
  rules; remove them — they chopped the page up). Sections separate by spacing.
- **Feature 1 — "A graph of memory, _nothing paywalled_"** + a ledger panel:
  rows `project.license → MIT, fully open [verified]`, `user.timezone →
  Europe/Lisbon [verified]`, `graph.enabled → true. no upsell`. The field-keys
  (`project.license` etc.) are **monospace** (genuine identifiers); values and
  tags are Inter.
- **Feature 2 — "Self-hosted. _You own the data._"** + a genuine **monospace
  `wrangler.toml` code block** (this is real config, so mono is correct):
  ```
  # wrangler.toml
  [[vectorize]]
  binding = "MEMORY_INDEX"
  [[d1_databases]]
  database_name = "your-brain"
  ✓ nothing leaves your Cloudflare account
  ```
- **How it works** — three cards labeled **First / Then / Over time** (not
  "01/02/03"): Deploy your vault / Connect your agents / It gets sharper. Copy
  per mockup.
- **Pricing** — Self-host **$0** and Hosted **$12/mo** (hosted featured). The
  hosted tier has no billing yet: its CTA is **Join the waitlist** (or contact),
  not a fake checkout. Note what you wired it to.
- **FAQ** — keep the four Q&As; the "how is it different" answer lands the point
  that tenant isolation is covered by an adversarial test suite, not asserted in
  a blog post. Accordion via CSS `grid-template-rows: 1fr/0fr` (no library).
- **Final CTA** — "Give your agents a memory _you control_" with a radial glow
  that **bleeds beyond the section** (no abrupt cutoff band — an earlier hard
  edge was flagged; let it diffuse). CTAs: Deploy your own, free + Read the docs.
- **Footer** — © MemoryVault · MIT licensed · "built on Cloudflare".
- **Panels** have a subtle top inner-light and soft shadow (see `.panel` in the
  mockup) so the side visuals feel considered, not flat.
- The product screenshot (the `/view` viewer) is the only raster on the page if
  you include it; ship it at retina (≥2× displayed width).

---

## 5. Mobile (must be fixed — earlier build broke here)

At ~390px the inline nav wrapped and pushed buttons offscreen. Fix per the
mockup:
- **Below 760px, collapse the nav into a hamburger button + a dropdown "sheet"**
  (translucent blurred dark panel) containing the links + GitHub + a full-width
  "Deploy free" button. Tapping a link closes the sheet; an X/active state on the
  button. (Markup + toggle JS are in the mockup: `#menu`, `#sheet`.)
- **Hero CTAs stack full-width** on mobile (don't crowd side by side).
- Headline scales down (~38px), section padding tightens, grids go single-column.
- **Star density capped on mobile** (perf rule above).
- Verify the whole page at 390px: nav, hero, CTAs, each section, footer.

---

## 6. Other public pages (calm constellation theme)

`/mcp`, endpoint guides, docs/endpoints, OAuth authorize/consent/login: same dark
tokens, type, nav, footer as the landing — but **no full animation** (static or
subtle stars, or just the gradient). Pull all styling from the shared theme
module (single source of truth); no page defines its own palette. OAuth: theme
only, behavior unchanged, form fully legible. Keep the developer endpoint
reference reachable (under Docs or `/endpoints`); don't delete it.

---

## 7. Verify (paste results in report)

```bash
npx tsc --noEmit
npm test
BASE_URL=http://127.0.0.1:8787 npm run test:isolation     # 20/20
grep -rn ": any\b\| as any\b" src/                         # empty
```
Screenshot at **1280×840** and **390×844** (the canvas animates, so capture
settled frames):
- landing hero (stars glowing, dead-zone clear behind text) — matches mockup,
- a feature section (the monospace `wrangler.toml` block + the ledger),
- how-it-works + the bled final-CTA gradient,
- **mobile hero with the hamburger**, and the **open mobile menu sheet**,
- an **OAuth screen** in the calm theme (form clearly legible).
Save under `mockup/shots/`, reference them, and self-critique against these:
no em-dashes in any copy; monospace only on code/config/field-keys; no legend; no
section dividers; gradient bleeds (no hard band); stars read as crisp glints
(not soft bloom) and never sit behind the text; mobile nav works; reduced-motion
renders a static field; OAuth/docs legible; AA contrast throughout.

---

## 8. Commits + PR description (human voice)

Coherent commits, e.g.:
- `theme: give the public site the constellation identity`
- `landing: build the living-graph hero with a text dead-zone`
- `landing: section copy, mono for code only, no em-dashes`
- `landing: collapse nav to a sheet on mobile`
- `landing: calm the starfield on utility and oauth pages`

PR description: the public site is now the dark "constellation" identity; the
landing hero is a live memory-graph starfield (vanilla canvas, no deps,
perf-capped, reduced-motion-safe) with a text dead-zone; monospace is used only
for real code/config; copy contains no em-dashes; sections flow without dividers
and the final gradient bleeds; mobile nav collapses to a sheet; OAuth and docs
use a calm version of the theme with behavior unchanged; `/view` theming
untouched.

---

## 9. Report back

1. Confirmation of the anti-AI-gen rules: zero em-dashes in prose, monospace only
   on code/config/field-keys, no decorative legend.
2. Star rendering + the performance caps (mobile density, offscreen/hidden pause,
   DPR/fps, reduced-motion static frame) and the dead-zone.
3. Which pages got full animation vs the calm theme; confirmation OAuth
   behavior/forms are unchanged and legible.
4. Mobile nav approach; confirmation the 390px layout works.
5. Pricing hosted-tier CTA wiring; confirmation the endpoint reference is still
   reachable.
6. The screenshots listed in §7.
7. tsc / test / isolation / grep output.
8. Proposed commit subjects/bodies + PR-description addition for review BEFORE
   pushing.
9. Anything intentionally deferred and why.

Stop and flag rather than proceed if any item would require a build step, a new
dependency, or any change to auth/isolation logic.
