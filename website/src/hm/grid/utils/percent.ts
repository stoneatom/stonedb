import {calc, valueOf} from './calc';

export function toPercent(precision: string) {
  const value = valueOf(calc(precision));
  return value > 1 ? '100%' : value * 100 + '%'
}