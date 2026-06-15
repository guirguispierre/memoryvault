# Fix: the hero→section seam (stars stop AND background shifts on the same line)

On the `pr39` landing page there's a visible horizontal seam where the hero meets
the first section. The user confirms it's **both** at once: the stars stop and
the background tone shifts at the **same row**, which stacks two soft edges into
one obvious line.

**Root cause (confirmed in `src/routes.ts`):**
- `.hero-wrap { height: 100vh }` and `#sky { height: 100vh; inset: 0 }` — the
  canvas ends exactly at the hero/`main` boundary, so stars terminate there.
- `main { background: var(--bg) }` with `main::before` ambient gradient starts at
  that same boundary, so the background tone also changes there.
- Both transitions resolve on the same horizontal line → perceived hard seam.

The earlier single mask attempt failed because it faded the stars but left both
transitions on the same row. The fix is to **stagger them** so no single line is
where everything changes, and make each transition gradual.

## Do this (CSS only — no JS, no canvas-logic change)

1. **Make the canvas taller than the hero and let it overflow into the first
   section**, so stars thin out *past* the boundary rather than stopping at it:
   ```css
   .hero-wrap { position: relative; height: 100vh; overflow: visible; }
   #sky {
     position: absolute; top: 0; left: 0; width: 100%;
     height: 135vh;            /* taller than the 100vh hero */
     z-index: 0; display: block; pointer-events: none;
     -webkit-mask-image: linear-gradient(180deg, #000 0%, #000 60%, transparent 88%);
             mask-image: linear-gradient(180deg, #000 0%, #000 60%, transparent 88%);
   }
   ```
   Now the stars fade out around 88% of 135vh — i.e. *below* the hero/section
   boundary, inside the first section's empty top area, not at the seam.

2. **Spread the background transition across a different, taller band** than the
   star fade, so the two don't coincide. Keep `main`'s base `--bg`, but bridge the
   tone with a gradient that begins above `main` and finishes well inside it:
   ```css
   main { position: relative; z-index: 1; background: var(--bg); }
   main::after {
     content: ""; position: absolute; top: -40vh; left: 0; right: 0;
     height: 60vh; z-index: 0; pointer-events: none;
     background: linear-gradient(180deg, transparent 0%, var(--bg) 70%);
   }
   ```
   (Keep `main::before` as is, or soften its top radial if it adds a lip. The
   `::after` must sit behind content; ensure section content is `z-index: 1`+.)

3. **Critical ordering check:** the overflowing canvas (`#sky`) must remain
   *visible over the very top of `main`* (so stars appear to drift into the
   section), but *behind* the section's text/cards. Verify z-index stacking:
   canvas at the hero's `z-index: 0`, hero text `1/2`, `main` content `1`. If the
   canvas overflow ends up painted under `main`'s opaque background and
   disappears, give the canvas a higher stacking context or make `main`'s top
   transparent for the overlap region. Adjust until stars are visibly thinning
   into the first section's top, with the background easing in on a *different*
   row than where the stars finish fading.

## Verify (in the real worker, not a reconstructed file)

Boot the actual worker and screenshot the hero→section boundary at **1280px** and
**390px**. Then sample a vertical pixel column on an empty left-edge strip
(x≈90px, away from text/panels) from the hero into the section: there should be
**no luminance step ≥6** in the background, and the stars should visibly thin out
at a *different* vertical position than where the background finishes darkening.
Paste the pixel-probe result (or describe the gradient) and save the screenshots
under `mockup/shots/`.

Do not change the star rendering, the rAF loop, the dead-zone, performance caps,
or any copy. CSS only, scoped to `.hero-wrap`, `#sky`, and `main`.

## Commit

`landing: stagger the hero starfield and background fade to remove the seam`
(human voice, lower-case, no trailing period, no tool trailers). One-line PR note
that the hero/section seam is resolved by extending+masking the canvas and
spreading the background transition across a different band.
