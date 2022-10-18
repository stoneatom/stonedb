import {toPercent} from './utils/percent';
import {columns} from './constant';

export function layout_cell_span(device: string, span: number, gutter: string) {
  let percent = toPercent(`${span} / ${columns[device]}`);
  return `
    width: calc(${percent} - ${gutter});
    width: calc(${percent} - var(--mdc-layout-grid-gutter-${device}, ${gutter}));
  `
}

export function layout_cell(device: string, span: number, gutter: string) {
  
  
  return `
    ${layout_cell_span(device, span, gutter)}
    box-sizing: border-box;
    margin: calc(${gutter} / 2);
    margin: calc(var(--mdc-layout-grid-gutter-${device}, ${gutter}) / 2);
  `
}

export function layout_cell_align(align: string) {
  if(align === 'top') {
    return `
      align-self: flex-start;
    `
  }
  if(align === 'middle') {
    return `
      align-self: center;
    `
  }
  if(align === 'bottom') {
    return `
      align-self: flex-end;
    `
  }
  if(align === 'stretch') {
    return `
      align-self: stretch;
    `
  }
  return null;
}