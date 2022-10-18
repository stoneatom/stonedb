import {columns} from '../constant';
import {breakpoint_min} from './breakpoint_min';
import {calc} from './calc';

export function breakpoint_max(device: string) {
  const keys = Object.keys(columns);
  const index = keys.indexOf(device);
  const prev = index > 1 ? keys[index - 1] : null;
  return prev ? calc(`${breakpoint_min(prev)} - 1px`) : null;
}