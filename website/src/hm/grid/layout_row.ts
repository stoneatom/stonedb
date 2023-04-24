import {css} from 'styled-components';

export function layout_row(device: string, margin: string, gutter: string) {
  return `
    display: flex;
    flex-flow: row wrap;
    align-items: stretch;
    margin: calc(${gutter} / 2);
    margin: calc(var(--mdc-layout-grid-gutter-${device}, ${gutter}) / 2 * -1);
  `
}