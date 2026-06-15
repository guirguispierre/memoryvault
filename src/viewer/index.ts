import { documentOpen, bodyMarkup } from './markup.js';
import { rootVariables, themeStyles } from './themes.js';
import { baseStyles } from './styles.js';
import { componentStyles } from './styles-components.js';
import { overlayStyles } from './styles-overlays.js';
import { themeRuntimeJs } from './tokens.js';
import { clientCore } from './client-core.js';
import { clientUi } from './client-ui.js';
import { clientSettings } from './client-settings.js';
import { clientGraph } from './client-graph.js';
import { clientOnboarding } from './client-onboarding.js';

export function viewerHtml(): string {
  return documentOpen + rootVariables + themeStyles + baseStyles + componentStyles + overlayStyles + bodyMarkup;
}

export function viewerScript(): string {
  // themeRuntimeJs first: it defines the shared theme helpers the client uses.
  return themeRuntimeJs + clientCore + clientUi + clientSettings + clientGraph + clientOnboarding;
}
