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
