import {BaseCompaonentProps} from '../default.interface';

export type GridAlignment = 'left' | 'right';
export type CellAlignment = 'bottom' | 'middle' | 'top';
export type TwelveColumn = 1 | 2 | 3 | 4 | 5 | 6 | 7 | 8 | 9 | 10 | 11 | 12;
export type FourColumn = 1 | 2 | 3 | 4;
export type EightColumn = 1 | 2 | 3 | 4 | 5 | 6 | 7 | 8;
export interface GridProps extends BaseCompaonentProps {
  align?: GridAlignment;
  fixedColumnWidth?: boolean;
}

export interface RowProps extends BaseCompaonentProps{
}

export interface CellProps extends BaseCompaonentProps{
  align?: CellAlignment;
  span?: TwelveColumn;
  order?: TwelveColumn;
}