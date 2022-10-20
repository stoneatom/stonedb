import {css} from 'styled-components';

export function layout_grid(device: string, margin: string, max_width = null) {
  return `
    box-sizing: border-box;
    margin: 0 auto;
    padding: ${margin};
    padding: var(--mdc-layout-grid-margin-${device}, ${margin});
    ${max_width ? `
      max-width: ${max_width};
    ` : null}
  `
}