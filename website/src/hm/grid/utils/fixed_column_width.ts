import {css} from 'styled-components';
import {columns} from '../constant';

export function fixed_column_width(device: string, margin: string, gutter: string, column_width: string) {
  const columnCount = columns[device];
  const gutter_number = columnCount! - 1;
  const margin_number = 2;
  return css`
    width: calc(${column_width} * ${columnCount} + ${gutter} * ${gutter_number} + ${margin} * ${margin_number});
    width: calc(
      var(--mdc-layout-grid-column-width-${device}, ${column_width}) * ${columnCount} +
        var(--mdc-layout-grid-gutter-${device}, ${gutter}) * ${gutter_number} +
        var(--mdc-layout-grid-margin-${device}, ${margin}) * ${margin_number}
    );
  `
}