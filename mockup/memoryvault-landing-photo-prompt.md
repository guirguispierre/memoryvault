# Task: the MemoryVault landing page at `/` — photographic hero + animations

Addition to PR #39 (or a fresh branch off it — your call). Build the public
marketing landing page at `/`, with a full-bleed **photographic hero** at the
top and the rest of the marketing content flowing **underneath** it on the same
page. This supersedes any earlier landing-page prompt — treat this as the single
source of truth for `/`.

> Interpretation of the request: the photo hero is the **top of the main `/`
> page**; the product shot, feature sections, pricing, FAQ, and footer stack
> **below** it on that same page. If you intended the hero as a separate page,
> stop and ask — but this is the assumed layout.

Reference mockups ship with this prompt — read them before coding and match
them closely:
- `mockup/landing-photo.html` — the target hero (photo background, blur-up
  headline, gradient + glow fade-to-page, glass nav, CTAs) plus the content
  sections below.
- `mockup/hero-bg.png` — the hero background image (provided by the user; ship
  it as a static asset).
- `mockup/paper-spectral.html` — the app viewer in the Spectral/paper look; its
  screenshot is the product shot embedded under the hero.

## Hard constraints (unchanged from all prior work — do not violate)

- **No React, no framework, no build step, no bundler.** The page is
  hand-written HTML/CSS + a small amount of vanilla JS, served as a string (or
  static asset) from the Worker, exactly like every other page. Adding a build
  pipeline is out of scope and not wanted.
- No new runtime dependencies. No CSS/JS libraries (no Tailwind runtime, no
  Framer Motion, no GSAP) — the animations are plain CSS keyframes + one
  `IntersectionObserver`.
- No changes to auth/OAuth logic, isolation, or `brain_id` scoping. This is a
  public, unauthenticated page; it touches none of the security surface.
- `npx tsc --noEmit` clean; `npm test` + live isolation suite green (20/20);
  no `any`, no dead/commented-out code, no emoji in chrome; comments only where
  non-obvious; human-voice commits and PR description per the PR #39 standard.

## Design system

Use the paper/Spectral tokens and type roles from `mockup/paper-spectral.html`
(Spectral `--doc` for headings/content, Inter `--ui` for chrome, JetBrains Mono
`--mono` for data/eyebrow/meta). Tokens:
```
--paper:#FBFAF7 --paper-2:#FFFFFF --paper-3:#F2F0EB
--rule:#E6E3DC --rule-soft:#EEEBE4
--ink:#1C1B19 --ink-dim:#6B6A66 --ink-faint:#9A988F
--accent:#3B5BDB --accent-soft:#E8ECFB --good:#2F8F6B
```
These should come from the shared theme module so the landing page stays
consistent with the app and with the site-wide default (paper). Light is the
default; provide dark-mode values for the page surfaces too if the site theme
resolves to dark.

## The hero (top of the page)

Reproduce `mockup/landing-photo.html`'s hero exactly in structure:

1. **Background image** — `hero-bg.png` served as a static asset from the Worker,
   `object-fit:cover; object-position:center`, full-bleed, `min-height` ~92vh.
   Serve it efficiently: emit a reasonably sized asset (and ideally a smaller
   variant for mobile via `srcset`, or use Cloudflare image resizing if
   available). Note in the report how you served it.
2. **Readability + fade overlays** (the technique that makes text legible and
   melts the photo into the page):
   - a top-down gradient wash over the photo (darker at top for nav/headline
     contrast, transparent by ~78%),
   - blurred near-white "glow" blobs at the bottom-left, bottom-right, and
     bottom-center,
   - a bottom fade strip from transparent → `--paper` so the hero dissolves into
     the page below.
   All values are in the mockup; match them.
3. **Glass nav** over the photo — wordmark `memoryvault.md` (white, subtle text
   shadow), center links (Features / How it works / Pricing / FAQ), right side a
   glass "Docs" and a solid white "Get started". Links are in-page anchors.
4. **Headline with per-letter blur-up reveal** — "The memory layer your agents
   *actually own*" (`actually own` italic + light tint). Each character is a
   `<span>` starting at `opacity:0; filter:blur(10px); translateY(10px)`,
   animating to clear/sharp/in-place with a staggered `animation-delay` (~0.03s
   per char). Generate the spans with a few lines of vanilla JS (as in the
   mockup) rather than hand-writing them. **Under `prefers-reduced-motion`, the
   text renders fully visible with no animation.**
5. **Hairline + subhead + CTAs + mono sub-line**, each fading/rising in on a
   short staggered delay after the headline. CTAs: solid white "Deploy your own
   — free" (with arrow) and a glass "View on GitHub". Sub-line: "MIT licensed ·
   graph included · deploy to Cloudflare in ~5 min".
   - **Fix from the mockup:** ensure the mono sub-line stays readable — it
     currently risks landing on the busy part of the photo. Nudge the hero
     content block up slightly and/or strengthen the gradient behind the
     sub-line so all hero text passes AA contrast against the image at all
     viewport sizes. Verify at 1280px and 390px.

## Below the hero (same page, flowing underneath)

In order, matching the mockup and the agreed content priority (graph +
nothing-paywalled first, self-hosted second, any-agent third, security as the
FAQ punch):

1. **Product shot** — the real paper-Spectral viewer screenshot in a
   bordered/shadowed frame, pulled up slightly to overlap the hero's fade so the
   transition reads as one composition.
2. **Feature 1 — "A graph of memory, nothing paywalled"** + ledger panel (key →
   value, verified tags).
3. **Feature 2 — "Self-hosted. You own the data."** + config-snippet panel
   (`[[vectorize]]` / `[[d1_databases]]` + "memories never leave your Cloudflare
   account").
4. **How it works** — three steps (Deploy your vault / Connect your agents /
   Watch it strengthen).
5. **Agents strip** — Claude, Claude Code, Codex, any MCP client, REST API.
6. **Pricing** — Self-host **$0** and Hosted **$12/mo** (hosted featured).
   Hosted has no billing yet: point its CTA at a waitlist/contact or label it
   "coming soon" rather than implying instant signup. Note what you wired it to.
7. **FAQ** — accordion (CSS `grid-template-rows: 1fr/0fr` transition + rotating
   chevron, as in the reference): open source? / data location? / agents? / how
   is it different (this one lands the "tenant isolation covered by an
   adversarial test suite, not asserted in a blog post" point).
8. **Final CTA** (accent solid button) + **footer** (© MemoryVault · MIT
   licensed · "all systems operational" ping dot).

## Animation layer (all CSS + vanilla JS, reduced-motion-safe)

- Per-letter hero blur-up (above).
- Scroll reveals: each major block below the hero starts faded/translated and
  animates in via a single `IntersectionObserver` toggling an `.in` class;
  unobserve after firing. Don't animate every tiny element — the section
  heading + its panel/figure per section is enough.
- Small hover transitions on buttons and step cards.
- FAQ accordion open/close transition.
- The ping dot uses a CSS `@keyframes` expanding ring.
- A single `@media (prefers-reduced-motion: reduce)` block disables all of it
  (opacity/transform/filter reset, `scroll-behavior:auto`) and the JS falls back
  to adding `.in` immediately + rendering letters visible.

## Wiring

- Route `/` to this page. Keep the developer endpoint reference that currently
  lives at `/` reachable elsewhere (under Docs or `/endpoints`) — don't delete
  it.
- Every CTA links somewhere real: GitHub repo, the `/view` app (Get started /
  Deploy), docs. In-page nav links are anchors. No dead `#` except anchors.
- Light/dark: the page honors the site theme via the shared bootstrap (paper
  default). If you include a toggle, persist via the same mechanism as the rest
  of the site; otherwise it follows the saved/`auto` theme. The hero text is
  light over the photo in both modes (the photo provides its own contrast); the
  content below follows the theme.
- Responsive: hero headline scales down, nav links collapse on mobile, feature
  grids stack, pricing cards stack, the photo still covers cleanly at 390px.

## Verification (paste in report)

```bash
npx tsc --noEmit
npm test
BASE_URL=http://127.0.0.1:8787 npm run test:isolation     # 20/20
grep -rn ": any\b\| as any\b" src/                         # empty
```

Extend the screenshot script (the page is animated, so capture settled states):
- hero at **1280** after animations settle (matches `landing-photo.html`),
- hero at **390** (mobile — photo covers, headline scaled, sub-line readable),
- the **product-shot transition** (scrolled so the shot overlaps the fade),
- a **mid-page section** after its scroll-reveal fired,
- the **FAQ** with one item open,
- if dark mode is supported, the hero + one section in dark.
Save under `mockup/shots/`, reference them, and self-critique vs the mockup:
all hero text passes contrast over the photo (especially the sub-line), the
fade-to-paper is seamless, reduced-motion renders everything static, mobile
reflow is clean.

## Commits / PR description (human voice)

- Coherent commits on the branch, e.g.
  `landing: add photographic hero with blur-up headline at /`,
  `landing: build feature, pricing, and faq sections below the hero`.
  Imperative, lower-case, <~70 chars, no trailing period, no tool trailers or
  emoji; short bodies on the why.
- Update the PR description: `/` is now a marketing landing page with a
  photographic hero and CSS/vanilla-JS animations (no framework, no build step),
  Spectral/paper design, light/dark aware; developer endpoint reference moved
  under Docs.

## Report

1. How the hero image asset is served (size/variants/resizing).
2. The hero contrast fix and how you verified AA over the photo.
3. The animation implementation (keyframes + observer) and the reduced-motion
   fallback.
4. What each CTA links to; how the hosted-tier CTA is handled given no billing.
5. Confirmation the old endpoint reference is still reachable.
6. The screenshots listed above.
7. `tsc` + test + isolation + grep output.
8. Proposed commit subjects/bodies and PR-description addition for review before
   pushing.
9. Anything intentionally deferred and why.

Stop and flag rather than proceed if any item would require a build step, a new
dependency, or any change to auth/isolation.
