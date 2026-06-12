# Engineering task: rebuild the MemoryVault viewer UI — "Dark Vanilla"

You are a senior frontend engineer redesigning the `/view` web UI of
MemoryVault (a Cloudflare Workers memory server). The current UI is a dark
"hacker-terminal vault" (amber-on-black, uppercase, glows, "ACCESS DENIED"). You
are replacing the visual identity with a calm, warm, premium dark-vanilla look.
**This is a reskin, not a behavior change** — every existing feature, action,
data flow, shortcut, and overlay must keep working exactly as before.

Two reference mockups ship with this prompt: `vanilla.png` (main list) and
`vanilla-login.png` (sign-in). Full source is in `mockup/vanilla.html` and
`mockup/vanilla-login.html` — read them for exact tokens, spacing, and markup
patterns, and port them into the worker's viewer modules. Match the references
closely; do not invent a different palette or fonts.

## The concept — cream poured into dark coffee

A warm espresso-brown ground (never blue-black) with everything rendered in one
vanilla family: cream ink, butter accent, latte mid-tones. The UI is
monochrome-warm and soft. The organizing idea carried over from the product
itself: **memory strength is shown as cream intensity** — actively reinforced
memories are bright vanilla; fading ones recede toward faded latte. Color never
encodes memory *type* (note/fact/journal is a small mono label only); the single
saturated moment in the whole UI is the butter accent (active filter tab,
primary button, focus ring, strength bars).

The list is grouped by state with soft, human tier names set in Fraunces italic:
**Active** ("reinforced this week"), **Settling** ("quiet for a few days"),
**Resting** ("fading — review soon"). Compute tiers from each memory's real
strength `(importance, recency, reinforcement)` — don't hardcode. If the backend
doesn't expose a single strength value, derive one client-side from the fields
it returns and document the formula in one comment.

**Each row:** a cream bead (filled butter = Active, deep-butter = Settling,
hollow latte ring = Resting), the title in Fraunces, content in Schibsted
Grotesk, and a right meta column in mono: accession id + time, a strength meter
(butter fill; latte fill in the Resting tier), linked-count in sage. **Facts:**
the title IS the key (e.g. `project.license`), and the body line is just
`→ value` — do not repeat the key in both places (the mockup has this flaw;
fix it).

### The signature element — "the pour"

A slim strip under the header: monochrome cream activity ticks over the last
24h, taller and more opaque toward "now" on the right, fading to near-nothing on
the left. Ambient, not interactive-heavy. Build it from real recent-activity
data if available, else from most-recently-updated memories. It replaces the old
stat-pills as the visual anchor; the entry/link counts move to its right-hand
caption. Keep this the one bold element — everything else stays quiet.

## Design tokens (use exactly)

Dark theme "vanilla-dark" (the default):
```
--ground:#181511   --ground-2:#201C16   --ground-3:#26211A
--rule:#332C22     --rule-soft:#2A2419
--cream:#F0E7D5    --cream-dim:#B5AB97  --cream-faint:#7E7666
--butter:#E3C98F   --butter-deep:#A98F5C
--latte:#8C8170    --sage:#9DB39A
```
Type roles (Google Fonts, one `<link>`):
- **Display** (wordmark, row titles, tier labels — tier labels italic):
  Fraunces, soft optical sizing (weights ~420/560/640). The wordmark is
  `Memory` roman + `Vault` italic in butter.
- **Body** (memory content, UI): Schibsted Grotesk (400/500/600).
- **Data** (ids, timestamps, type labels, meters, the pour caption):
  IBM Plex Mono (400/500).

Do not substitute fonts. Corners are soft (8–14px radii), shadows deep and
warm, spacing generous — the design's quality lives in spacing precision, so be
exact about the reference's rhythm.

## Voice

Calm, human, sentence case. "ACCESS DENIED" → a plain inline error that says
what to do. The login reads "Open your index — a second brain you host
yourself"; the primary button is "Open index"; the agent hint is "Connecting an
agent? Use a bearer token instead." Errors explain and instruct. Empty states
invite ("Nothing here yet — save your first memory."). No emoji in chrome.

## Accessibility floor (this concept's main risk)

"Fading = faint" must never break readability. The Resting tier expresses decay
through the hollow bead, the latte meter, and the tier label — **not** by
dropping body-text contrast: text in every tier must hold **WCAG AA** against
`--ground` (verify Resting explicitly; lighten `--cream-dim`/`--latte` usage on
text if it fails). Visible keyboard focus ring in `--butter`;
`prefers-reduced-motion` disables any pour entrance animation and transitions;
AA contrast throughout.

## Where the code lives

Server-rendered HTML/CSS/JS strings under `src/viewer/`: `markup.ts`,
`styles.ts` + `styles-components.ts` + `styles-overlays.ts`, `themes.ts`
(tokens + variants), `client-*.ts` (behavior), assembled by `index.ts`. Read all
of them first and write a one-paragraph structure map before changing anything.

## Hard constraints

1. **Behavior frozen.** Every `data-action`, element `id`, handler, API call,
   keyboard shortcut, command-palette entry, settings toggle, graph interaction,
   and live-update path keeps working. Renaming an `id`/`data-action` means
   updating every reader in the same commit.
2. **No new runtime deps, no build step.** The viewer ships as strings from a
   Worker; hand-written CSS only. (Playwright for screenshots is a devDependency
   or `npx`-only.)
3. **Keep the theme system.** "vanilla-dark" becomes the default; port the
   existing light variants onto the new token names (a "vanilla-light" with
   cream ground and espresso ink is the natural counterpart) and keep the theme
   switcher working. Don't silently drop themes.
4. **`npx tsc --noEmit` clean and all suites green** (`npm test` plus the live
   isolation suite). UI work must not touch auth, isolation, or `brain_id`
   scoping.
5. **Responsive to 390px.** The row's meta column stacks under the content on
   phones; the pour stays legible with fewer ticks; the segmented filter scrolls
   horizontally or collapses to a select.
6. **No vibe-coded tells.** No `any`, no dead or commented-out CSS, no emoji in
   chrome, no lorem in shipped code; comments only for non-obvious decisions
   (e.g. the strength formula). Watch CSS specificity so element- and
   class-selectors don't cancel on section spacing. Keep the diff scoped to
   `src/viewer/`.

## Screenshot verification (required — I review on mobile)

I can't read diffs right now; show me the result.
1. Add `mockup/verify-ui.mjs`: boots the worker locally (the isolation wrangler
   config is fine — the viewer needs no Vectorize), seeds an account plus
   several memories of different types and ages via signup + `memory_save`, then
   screenshots with Playwright at `device_scale_factor=2`, waiting ~2.8s for
   webfonts:
   - **login** 1280×820,
   - **main list** 1280×980 (populated; all three tiers and the pour visible),
   - **main list** 390×844 (mobile reflow).
2. Save under `mockup/shots/` and reference them in your report.
3. **Self-critique vs the references** before declaring done: compare your shots
   to `vanilla.png` / `vanilla-login.png` — type scale, spacing rhythm, the
   pour, bead states, the butter accent discipline (it should appear in only a
   handful of places). Fix drift and re-shoot; note what changed between passes.

## Sequencing

1. Read `src/viewer/`; write the structure map.
2. Tokens into `themes.ts` (vanilla-dark default + vanilla-light + migrated
   variants), then base/layout CSS in `styles.ts`.
3. `markup.ts`: header + segmented filter, the pour, tier-grouped rows with
   beads + meters, login. Preserve every `id`/`data-action`.
4. Wire the client: per-memory strength + tier, the pour from recent activity,
   all existing behavior intact.
5. Restyle overlays (command palette, settings, shortcuts, graph toolbar) onto
   the new tokens so nothing looks orphaned.
6. Verify: `tsc`, `npm test`, boot + seed + screenshot all three, self-critique,
   re-shoot if needed.
7. Final report: the screenshots, the structure map, every `id`/`data-action`
   touched with confirmation its readers were updated, the strength-tier
   formula, and `tsc`/test output.

If any step would require changing behavior, auth, or isolation to achieve the
look, stop and flag it instead of doing it.
