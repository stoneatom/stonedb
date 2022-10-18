import {breakpoint_min} from './breakpoint_min';
import {breakpoint_max} from './breakpoint_max';

export function media_query(device: string, cb: Function) {
  const min = breakpoint_min(device);
  const max = breakpoint_max(device);
  if(!min && !max){
    return `
      // Phone
      @media (max-width: ${max}) {
        ${cb()}
      }
    `
  } else if (min && max) {
    return `
      // Tablet
      @media (min-width: ${min}) and (max-width: ${max}) {
        ${cb()}
      }
    `
  } else if (min && !max) {
    return `
      // Desktop
      @media (min-width: ${min}) {
        ${cb()}
      }
    `
  } else {
    return `
      // Fallback - no breakpoints defined
      ${cb()}
    `
  }
}