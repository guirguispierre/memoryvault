import { vanillaTokensCss } from './tokens.js';

// The :root token block lives in tokens.ts so the server-rendered pages can
// share it; the theme variants below extend it.
export const rootVariables = vanillaTokensCss + `
  /* ── THEME VARIANTS ── */
`;

export const themeStyles = `  [data-theme="vanilla-light"] {
    --ground: #F4EDDE;
    --ground-2: #FBF6EA;
    --ground-3: #EDE4D0;
    --rule: #DCD2BD;
    --rule-soft: #E4DBC8;
    --rule-bright: #C9BCA1;
    --cream: #2A241A;
    --cream-dim: #564C3B;
    --cream-faint: #877D6C;
    --butter: #C7A35C;
    --butter-deep: #8F713B;
    --latte: #9C9078;
    --sage: #5F7D5C;
    --clay: #A85B44;
    --on-butter: #241C0D;
    --butter-glow: rgba(169, 143, 92, 0.10);
  }
  [data-theme="vanilla-light"] body {
    background: radial-gradient(1000px 460px at 50% -12%, rgba(169, 143, 92, 0.08), transparent 62%), var(--ground);
  }

  [data-theme="midnight"] {
    --ground: #14141F;
    --ground-2: #1B1B2A;
    --ground-3: #222134;
    --rule: #2F2D45;
    --rule-soft: #282639;
    --rule-bright: #403D5C;
    --cream: #E4E2F2;
    --cream-dim: #A8A5C0;
    --cream-faint: #6F6C8A;
    --butter: #A99BE8;
    --butter-deep: #6F63B0;
    --latte: #807D9A;
    --sage: #8FB3A8;
    --clay: #C97E8E;
    --on-butter: #16122E;
    --butter-glow: rgba(169, 155, 232, 0.08);
    --panel-bg: #1B1B2A;
    --surface: rgba(30, 30, 46, 0.6);
    --surface-raised: #201F30;
    --toast-bg: #201F30;
    --overlay-bg: rgba(10, 10, 18, 0.78);
  }
  [data-theme="midnight"] body {
    background: radial-gradient(1000px 460px at 50% -12%, rgba(169, 155, 232, 0.07), transparent 62%), var(--ground);
  }
  [data-theme="midnight-light"] {
    --ground: #F0EEF8;
    --ground-2: #FAF9FE;
    --ground-3: #E7E4F2;
    --rule: #D3CFE4;
    --rule-soft: #DDD9EB;
    --rule-bright: #B9B3D4;
    --cream: #26224A;
    --cream-dim: #4C4870;
    --cream-faint: #7E7AA0;
    --butter: #7C6AD0;
    --butter-deep: #574A9E;
    --latte: #8C89A8;
    --sage: #56806E;
    --clay: #A8556A;
    --on-butter: #F4F2FC;
    --butter-glow: rgba(124, 106, 208, 0.10);
  }
  [data-theme="midnight-light"] body {
    background: radial-gradient(1000px 460px at 50% -12%, rgba(124, 106, 208, 0.08), transparent 62%), var(--ground);
  }

  [data-theme="solarized"] {
    --ground: #002B36;
    --ground-2: #073642;
    --ground-3: #0A3F4C;
    --rule: #15505E;
    --rule-soft: #0E4654;
    --rule-bright: #226676;
    --cream: #EEE8D5;
    --cream-dim: #93A1A1;
    --cream-faint: #5E767D;
    --butter: #B58900;
    --butter-deep: #8A6A00;
    --latte: #657B83;
    --sage: #2AA198;
    --clay: #DC322F;
    --on-butter: #00232C;
    --butter-glow: rgba(181, 137, 0, 0.09);
    --panel-bg: #073642;
    --surface: rgba(7, 54, 66, 0.6);
    --surface-raised: #0A3B48;
    --toast-bg: #0A3B48;
    --overlay-bg: rgba(0, 26, 33, 0.78);
  }
  [data-theme="solarized"] body {
    background: radial-gradient(1000px 460px at 50% -12%, rgba(181, 137, 0, 0.07), transparent 62%), var(--ground);
  }
  [data-theme="solarized-light"] {
    --ground: #FDF6E3;
    --ground-2: #FFFCF2;
    --ground-3: #F3EAD2;
    --rule: #DCD0AC;
    --rule-soft: #E6DCBE;
    --rule-bright: #C5B788;
    --cream: #073642;
    --cream-dim: #50626A;
    --cream-faint: #84928F;
    --butter: #A37B00;
    --butter-deep: #7A5C00;
    --latte: #93A1A1;
    --sage: #3D8B84;
    --clay: #C03330;
    --on-butter: #FFF9E8;
    --butter-glow: rgba(163, 123, 0, 0.10);
  }
  [data-theme="solarized-light"] body {
    background: radial-gradient(1000px 460px at 50% -12%, rgba(163, 123, 0, 0.08), transparent 62%), var(--ground);
  }

  [data-theme="ember"] {
    --ground: #1A0E0A;
    --ground-2: #231410;
    --ground-3: #2C1A14;
    --rule: #46291F;
    --rule-soft: #3A221A;
    --rule-bright: #5C372A;
    --cream: #F3E3DA;
    --cream-dim: #C4A99C;
    --cream-faint: #8A6E61;
    --butter: #E8956A;
    --butter-deep: #B05F38;
    --latte: #8F7264;
    --sage: #B3A284;
    --clay: #D96D5C;
    --on-butter: #2A130A;
    --butter-glow: rgba(232, 149, 106, 0.08);
    --panel-bg: #231410;
    --surface: rgba(38, 22, 17, 0.6);
    --surface-raised: #281712;
    --toast-bg: #281712;
    --overlay-bg: rgba(18, 8, 5, 0.78);
  }
  [data-theme="ember"] body {
    background: radial-gradient(1000px 460px at 50% -12%, rgba(232, 149, 106, 0.07), transparent 62%), var(--ground);
  }
  [data-theme="ember-light"] {
    --ground: #FAF0E8;
    --ground-2: #FFF9F4;
    --ground-3: #F2E4D8;
    --rule: #E0CCBE;
    --rule-soft: #E8D8CC;
    --rule-bright: #CBB0A0;
    --cream: #3A2014;
    --cream-dim: #64483A;
    --cream-faint: #93786B;
    --butter: #C0622E;
    --butter-deep: #94481E;
    --latte: #A08876;
    --sage: #8A7848;
    --clay: #B03A28;
    --on-butter: #FFF6EE;
    --butter-glow: rgba(192, 98, 46, 0.10);
  }
  [data-theme="ember-light"] body {
    background: radial-gradient(1000px 460px at 50% -12%, rgba(192, 98, 46, 0.08), transparent 62%), var(--ground);
  }

  [data-theme="arctic"] {
    --ground: #0F1B22;
    --ground-2: #16242E;
    --ground-3: #1D2F3A;
    --rule: #2A4350;
    --rule-soft: #233A46;
    --rule-bright: #3A5868;
    --cream: #E2EEF2;
    --cream-dim: #A3BCC6;
    --cream-faint: #647F8B;
    --butter: #8FC7D8;
    --butter-deep: #54899A;
    --latte: #76909B;
    --sage: #93C7AE;
    --clay: #CF7E8A;
    --on-butter: #0A1A20;
    --butter-glow: rgba(143, 199, 216, 0.08);
    --panel-bg: #16242E;
    --surface: rgba(22, 36, 46, 0.6);
    --surface-raised: #1A2A35;
    --toast-bg: #1A2A35;
    --overlay-bg: rgba(7, 14, 18, 0.78);
  }
  [data-theme="arctic"] body {
    background: radial-gradient(1000px 460px at 50% -12%, rgba(143, 199, 216, 0.07), transparent 62%), var(--ground);
  }
  [data-theme="arctic-light"] {
    --ground: #EEF5FA;
    --ground-2: #FAFCFE;
    --ground-3: #E2ECF4;
    --rule: #C9D8E4;
    --rule-soft: #D4E0EA;
    --rule-bright: #A8BFD0;
    --cream: #16303E;
    --cream-dim: #3E586A;
    --cream-faint: #6F8694;
    --butter: #1F7E9A;
    --butter-deep: #145B70;
    --latte: #7E94A2;
    --sage: #4A8268;
    --clay: #AD4A60;
    --on-butter: #F2F9FC;
    --butter-glow: rgba(31, 126, 154, 0.10);
  }
  [data-theme="arctic-light"] body {
    background: radial-gradient(1000px 460px at 50% -12%, rgba(31, 126, 154, 0.08), transparent 62%), var(--ground);
  }

  /* Slate: neutral clean-dark default. True grays with one muted slate-blue
     accent; minimal hue so it reads calm rather than warm. */
  [data-theme="slate"] {
    --ground: #0F1011;
    --ground-2: #161719;
    --ground-3: #1C1E20;
    --rule: #2A2C2F;
    --rule-soft: #202224;
    --rule-bright: #3A3D41;
    --cream: #E6E7E8;
    --cream-dim: #A0A2A6;
    --cream-faint: #6A6D72;
    --butter: #7FA6C9;
    --butter-deep: #5A7E9E;
    --latte: #8A8D92;
    --sage: #8FB3A8;
    --clay: #C98E8E;
    --on-butter: #0E1518;
    --butter-glow: rgba(127, 166, 201, 0.06);
    --panel-bg: #161719;
    --surface: rgba(28, 30, 32, 0.6);
    --surface-raised: #1A1C1E;
    --toast-bg: #1A1C1E;
    --overlay-bg: rgba(8, 9, 10, 0.78);
  }
  [data-theme="slate"] body {
    background: radial-gradient(1000px 460px at 50% -12%, rgba(127, 166, 201, 0.05), transparent 62%), var(--ground);
  }
  [data-theme="slate-light"] {
    --ground: #F4F5F6;
    --ground-2: #FBFBFC;
    --ground-3: #EBECEE;
    --rule: #D8DADD;
    --rule-soft: #E3E4E7;
    --rule-bright: #C2C5C9;
    --cream: #1A1C1E;
    --cream-dim: #4A4D52;
    --cream-faint: #7C8086;
    --butter: #3E6E99;
    --butter-deep: #2E5374;
    --latte: #8A8D92;
    --sage: #4E7A6A;
    --clay: #A85B5B;
    --on-butter: #F4F8FB;
    --butter-glow: rgba(62, 110, 153, 0.09);
  }
  [data-theme="slate-light"] body {
    background: radial-gradient(1000px 460px at 50% -12%, rgba(62, 110, 153, 0.07), transparent 62%), var(--ground);
  }

  /* Constellation: the public-site dark identity as a calm, static /view theme.
     Same deep-space palette, Spectral content / Inter UI / JetBrains Mono code,
     and the blue accent. No animation: the body carries one faint static wash,
     the same single-radial pattern every other theme already uses. Dark only,
     so it is offered as a dark base and never gets a -light variant. */
  [data-theme="constellation"] {
    --ground: #070810;
    --ground-2: #0B0D18;
    --ground-3: #11131F;
    --rule: rgba(255, 255, 255, 0.08);
    --rule-soft: rgba(255, 255, 255, 0.05);
    --rule-bright: rgba(255, 255, 255, 0.14);
    --cream: #F5F4EF;
    --cream-dim: #9AA0B4;
    --cream-faint: #565D72;
    --butter: #8AB0FF;
    --butter-deep: #5E7FD0;
    --latte: #9AA0B4;
    --sage: #86E0B8;
    --clay: #EF9D9D;
    --on-butter: #070810;
    --butter-glow: rgba(138, 176, 255, 0.08);
    --panel-bg: #0B0D18;
    --surface: rgba(255, 255, 255, 0.035);
    --surface-raised: #11131F;
    --toast-bg: #11131F;
    --overlay-bg: rgba(4, 5, 12, 0.82);
    --disp: 'Spectral', Georgia, serif;
    --body: 'Inter', system-ui, sans-serif;
    --mono: 'JetBrains Mono', ui-monospace, monospace;
  }
  [data-theme="constellation"] body {
    background:
      radial-gradient(1000px 480px at 50% -12%, rgba(40, 52, 110, 0.20), transparent 62%),
      radial-gradient(760px 520px at 88% 108%, rgba(30, 80, 70, 0.08), transparent 60%),
      var(--ground);
  }

  /* Paper: warm near-white reading surface with Spectral for content, Inter for
     UI chrome, JetBrains Mono for data, and one restrained indigo accent. This
     is the default light theme; the generic -light block below fills the panel
     and surface tokens. */
  [data-theme="paper-light"] {
    --ground: #FBFAF7;
    --ground-2: #FFFFFF;
    --ground-3: #F2F0EB;
    --rule: #E6E3DC;
    --rule-soft: #EEEBE4;
    --rule-bright: #D8D4CB;
    --cream: #1C1B19;
    --cream-dim: #6B6A66;
    --cream-faint: #9A988F;
    --butter: #3B5BDB;
    --butter-deep: #2C46AE;
    --latte: #8A8780;
    --sage: #2F8F6B;
    --clay: #C0533B;
    --on-butter: #FFFFFF;
    --butter-glow: rgba(59, 91, 219, 0.08);
    --disp: 'Spectral', Georgia, serif;
    --body: 'Inter', system-ui, sans-serif;
    --mono: 'JetBrains Mono', ui-monospace, monospace;
  }
  [data-theme="paper-light"] body {
    background: radial-gradient(1000px 460px at 50% -12%, rgba(59, 91, 219, 0.05), transparent 62%), var(--ground);
  }
  /* Dark counterpart, only used if paper is picked as the dark theme. */
  [data-theme="paper"] {
    --ground: #16161A;
    --ground-2: #1D1D22;
    --ground-3: #25252B;
    --rule: #30303A;
    --rule-soft: #28282F;
    --rule-bright: #3E3E4A;
    --cream: #ECEBE6;
    --cream-dim: #A6A59E;
    --cream-faint: #74736C;
    --butter: #8AA0F0;
    --butter-deep: #5E76C8;
    --latte: #8A8A82;
    --sage: #5FB894;
    --clay: #D08A78;
    --on-butter: #14141A;
    --butter-glow: rgba(138, 160, 240, 0.09);
    --disp: 'Spectral', Georgia, serif;
    --body: 'Inter', system-ui, sans-serif;
    --mono: 'JetBrains Mono', ui-monospace, monospace;
    --panel-bg: #1D1D22;
    --surface: rgba(37, 37, 43, 0.6);
    --surface-raised: #212128;
    --toast-bg: #212128;
    --overlay-bg: rgba(10, 10, 13, 0.78);
  }
  [data-theme="paper"] body {
    background: radial-gradient(1000px 460px at 50% -12%, rgba(138, 160, 240, 0.06), transparent 62%), var(--ground);
  }

  [data-theme$="-light"] {
    --overlay-bg: rgba(30, 26, 18, 0.30);
    --panel-bg: var(--ground-2);
    --panel-shadow: rgba(30, 24, 14, 0.14);
    --surface: var(--ground-3);
    --surface-raised: var(--ground-2);
    --toast-bg: var(--ground-2);
    --card-glow: rgba(30, 24, 14, 0.10);
  }
  [data-theme$="-light"] .login-box,
  [data-theme$="-light"] .cmd-box,
  [data-theme$="-light"] .shortcuts-box,
  [data-theme$="-light"] .settings-box,
  [data-theme$="-light"] .changelog-box,
  [data-theme$="-light"] .expand-box {
    box-shadow: 0 14px 40px rgba(30, 24, 14, 0.14);
  }
  [data-theme$="-light"] .toast {
    box-shadow: 0 6px 18px rgba(30, 24, 14, 0.12);
  }
`;
