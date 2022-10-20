import {breakpoints} from '../constant';
import {valueOf} from './calc';

export function breakpoint_min(device: string) {
  const min = breakpoints[device] as string;
  return valueOf(min) > 0 ? min : 0;
}