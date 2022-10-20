import styled, {createGlobalStyle, css} from 'styled-components';
import {media_query} from './utils/media_query';
import {layout_grid} from './layout_grid';
import {layout_row} from './layout_row';
import {layout_cell, layout_cell_align, layout_cell_span} from './layout_cell';
import {fixed_column_width} from './utils/fixed_column_width';
import {columns, margins, max_width, gutters, column_span, column_widths} from './constant';
import {GridProps, CellProps, CellAlignment} from './interface';

const devices = Object.keys(columns);

const GlobalStyle = createGlobalStyle`
  :root {
    --mdc-layout-grid-margin-desktop: 24px;
    --mdc-layout-grid-gutter-desktop: 24px;
    --mdc-layout-grid-column-width-desktop: 72px;
    --mdc-layout-grid-margin-tablet: 16px;
    --mdc-layout-grid-gutter-tablet: 16px;
    --mdc-layout-grid-column-width-tablet: 72px;
    --mdc-layout-grid-margin-phone: 16px;
    --mdc-layout-grid-gutter-phone: 16px;
    --mdc-layout-grid-column-width-phone: 72px;
  }
`;

const alignLeft = css`
  margin-right: auto;
  margin-left: 0;
`;
const alignRight = css`
  margin-right: 0;
  margin-left: auto;
`;

const layout_grid_fixed_column_width = css`
  ${
    devices.map((device: string) => media_query(device, () => {
      const margin = margins[device]!;
      const gutter = gutters[device]!;
      const column_width = column_widths[device]!;
      return fixed_column_width(device, margin, gutter, column_width);
    }))
  }
`;

export const GridWrap = styled.div<GridProps>`
  ${
    devices.map((device: string) => media_query(device, () => {
      const margin = margins[device]!;
      return layout_grid(device, margin, max_width);
    }))
  }
  ${
    ({align}: GridProps) => align === 'left' ? alignLeft : align === 'right' ? alignRight : null
  }
  ${
    ({fixedColumnWidth}: GridProps) => fixedColumnWidth ? layout_grid_fixed_column_width : null
  }
`
export const RowWrap = styled.div`
  ${
    devices.map((device: string) => media_query(device, () => {
      const margin = margins[device]!;
      const gutter = gutters[device]!;
      return layout_row(device, margin, gutter);
    }))
  }
`


export const CellWrap = styled.div<CellProps>`
  ${
    ({span}: CellProps) => {
      return devices.map((device: string) => media_query(device, () => {
        const gutter = gutters[device]!;
        let res = layout_cell(device, column_span, gutter);
        if(span) {
          res += layout_cell_span(device, span as number, gutter);
        }
        return res;
      }));
    }
  }
  ${
    ({align}: CellProps) => layout_cell_align(align as CellAlignment)
  }
  
`
