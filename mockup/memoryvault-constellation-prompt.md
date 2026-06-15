# Task: "Constellation" — a living-graph hero, and a dark constellation identity for the whole public site

Addition to PR #39. Replace the public-site look with a distinctive
**"living memory"** concept: the landing page hero is a dark night-sky of glowing
stars (your memory graph) that drift, twinkle, link, and parallax; and every
non-`/view` page adopts the same dark "constellation" identity so the whole
public site feels like one world. `/view` (the app) is untouched and keeps its
own theme system.

Reference mockup ships with this prompt — match its look and the star rendering
closely:
- `mockup/constellation.html` — the target hero (deep-space gradient, glowing
  radial stars with white cores, sparkle spikes on larger stars, faint linking
  lines, twinkle, cursor parallax, the legend mapping star colors to memory
  tiers).
- `mockup/constellation-1.png`, `constellation-2.png` — rendered references.
- `mockup/paper-spectral.html` — the `/view` app (do not restyle it; only
  referenced so you keep its theme system separate).

## Important scope clarification — read before building

"All pages but `/view` look like this" is correct for the **identity** (dark
space palette, Spectral type, the constellation theme), but the **full animated
starfield belongs only on the landing hero**, not behind login forms or docs.
Putting a live animated canvas behind the OAuth login/consent screens or the
docs/guide pages hurts readability, distracts where people are reading or typing,
and wastes battery. So:

- **Landing `/`**: full living-graph hero — the animated, glowing, drifting,
  parallax starfield (the centerpiece).
- **All other non-`/view` pages** (`/mcp`, endpoint guides, docs/endpoints, and
  the **OAuth** authorize/consent/login screens): the **same dark constellation
  theme** — same deep-space background gradient, same color palette, same
  Spectral/Inter/JetBrains-Mono type, same nav/footer — but **calm**: a static
  or very subtle low-density starfield (or just the gradient), never the
  full-intensity animation, so forms and text stay perfectly legible.
- This replaces the earlier "public pages are light paper" decision: the public
  site is now the dark constellation identity. `/view` still owns its own themes.
- If you genuinely want the full animation on every page including login, stop
  and confirm — but the above is strongly recommended and is the assumed spec.

## Hard constraints (unchanged)

- No React, no framework, no build step, no new dependencies. The starfield is
  hand-written **vanilla JS on a `<canvas>`**; everything else is hand-written
  HTML/CSS served as strings from the Worker.
- No changes to auth/OAuth logic, isolation, or `brain_id` scoping. The OAuth
  screens get the new *theme* only — forms, actions, hidden fields, redirects,
  button name/values stay byte-identical.
- `npx tsc --noEmit` clean; `npm test` + live isolation green (20/20); no `any`,
  no dead code, no emoji in chrome; comments only where non-obvious; human-voice
  commits and PR description per the PR #39 standard.

## The constellation theme (shared tokens)

Define these once in the shared theme module so every public page uses them:
```
--bg:#070810  --ink:#F5F4EF  --dim:#9AA0B4  --faint:#565D72
--accent:#8AB0FF (reinforced/blue)  --good:#86E0B8 (active/green)
--warm:#FFCAA0 (settling/amber)  --fade:#565D72 (fading)
--doc:Spectral  --ui:Inter  --mono:JetBrains Mono
```
Deep-space background gradient (behind everything, `z-index:-1`): layered soft
radial gradients — a blue wash top-center, faint green lower-right, faint violet
lower-left — over `--bg`. See the mockup for exact stops.

## The living-graph hero (landing `/`)

Reproduce `mockup/constellation.html`'s canvas exactly. Key rendering details
that make the stars look bright/shiny (do not flatten them to plain dots):

- **Each star is a radial gradient** (colored glow → transparent) with a **bright
  white core** on top — lit-from-within, not a flat fill.
- **Additive blending**: set `ctx.globalCompositeOperation='lighter'` while
  drawing stars and links so overlapping light adds up (the luminous look),
  then reset to `'source-over'`.
- **A minority (~15%) of "hero" stars** are larger and brighter and get
  **cross-shaped sparkle spikes**; the rest are small and dim. Hierarchy, not
  uniform dots.
- **Independent twinkle**: each star varies its brightness on its own slow sine
  cycle (random phase + rate) so the field shimmers rather than pulsing in
  unison.
- **Slow drift** + **cursor parallax**: stars move slowly and the whole field
  shifts subtly toward the pointer (depth via per-star depth factor). Wrap stars
  around edges.
- **Linking lines**: faint blue lines between stars within a threshold distance,
  opacity scaled by proximity — this is the "graph."
- **Color = memory state**: stars are colored from the tier palette (active /
  settling / fading / reinforced); the hero includes a small legend mapping the
  colors to those states, tying the visual to the real product.

Hero content over the canvas: mono eyebrow ("a living memory for your agents"),
Spectral headline "Memory that *strengthens*, fades, and connects itself."
(`strengthens` italic + accent), subhead ("Not a static file — a graph that
reinforces what matters, lets the rest decay, and gives every agent the same
evolving picture of you."), two CTAs (solid "Deploy your own — free" + glass
"View on GitHub"), and the tier legend. Headline sits in a subtle glow/`text-shadow`
so it stays readable over the field.

## Performance + accessibility (mandatory — this is an animated canvas)

- **Cap density by viewport**: far fewer stars on mobile (scale star count to
  width); never thousands.
- **Pause when offscreen/hidden**: stop the `requestAnimationFrame` loop when the
  hero is scrolled out of view (IntersectionObserver) and when
  `document.hidden` (tab not visible).
- **Cap DPR** at ~2 and consider throttling to ~30–45fps; keep it smooth on a
  mid laptop and a phone.
- **`prefers-reduced-motion`**: no motion at all — render a single static frame
  of the starfield (still pretty, no drift/twinkle/parallax).
- The canvas is decorative: `aria-hidden`, pointer-events none; all nav/CTAs
  remain keyboard-focusable and clickable above it.

## Sections below the hero (landing)

Keep the existing content/order (features: graph/nothing-paywalled + self-hosted;
how-it-works 3 steps; pricing Self-host $0 / Hosted $12/mo with hosted featured
and a "Join the waitlist" CTA; FAQ with the isolation-test punch; final CTA;
footer). Restyle them to the dark constellation theme. **The starfield fades out
below the hero** — the content sections sit on the calmer deep-space gradient (or
near-solid `--bg`) so the page is alive at the top and legible/restful below.
Cards/panels use a subtle raised dark surface with hairline borders; the product
shot (the `/view` screenshot) sits in a dark frame with a soft glow. The product
screenshot is the only raster — ship it at retina resolution.

## Other public pages (calm constellation theme)

`/mcp`, endpoint guides, docs/endpoints, and the OAuth authorize/consent/login
screens: same dark background gradient, palette, type, nav, and footer as the
landing — but **no full animation** (static/subtle stars or just the gradient).
OAuth especially must keep its form perfectly legible and its behavior unchanged.
Pull all styling from the shared theme module (single source of truth); no page
defines its own palette.

## Verify

```bash
npx tsc --noEmit
npm test
BASE_URL=http://127.0.0.1:8787 npm run test:isolation     # 20/20
grep -rn ": any\b\| as any\b" src/                         # empty
```
Screenshot at **1280×840** and **390×844**:
- the living-graph hero (stars visibly glowing, lines present) — matches the
  mockup,
- a content section below the hero (calm, legible),
- the **OAuth login screen** in the calm constellation theme (form clearly
  readable),
- mobile hero (reduced star density, headline scaled, still smooth).
Save under `mockup/shots/`, reference them, and self-critique: stars read as
glowing (not flat), headline/text pass AA over the background, hero is smooth and
calms below, OAuth/docs are legible, reduced-motion renders a static field, and
mobile density is capped.

## Commits / PR description (human voice)

- e.g. `landing: build the living constellation hero`,
  `theme: give the public site the dark constellation identity`,
  `landing: calm the starfield below the hero and on utility pages`.
  Imperative, lower-case, <70 chars, no trailing period, no tool trailers/emoji;
  short bodies on the why.
- PR description: the public site now uses a dark "constellation" identity; the
  landing hero is a live, glowing memory-graph starfield (vanilla canvas, no
  deps), calmed on content sections and utility/OAuth pages for legibility;
  performance-capped and reduced-motion-safe; `/view` theming unchanged; OAuth
  behavior unchanged.

## Report

1. The star-rendering approach (glow gradient + core + additive blend + spikes +
   twinkle + parallax) and the performance caps (mobile density, offscreen/hidden
   pause, DPR/fps, reduced-motion static frame).
2. Which pages got full animation vs calm theme, and confirmation OAuth
   forms/behavior are unchanged.
3. Product-shot resolution and how served.
4. Screenshots (desktop hero, section, OAuth, mobile).
5. tsc/test/isolation/grep output.
6. Proposed commits + PR-description note for review before pushing.
7. Anything intentionally deferred and why.

Stop and flag rather than proceed if any item would require a build step, a new
dependency, or any change to auth/isolation.
