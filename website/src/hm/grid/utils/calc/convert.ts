import {conversions} from './constant';

export function convert(value: number, sourceUnit: string, targetUnit: string, precision = '') {
  if(isNaN(value)) {
    return value;
  }
  if (!conversions.hasOwnProperty(targetUnit)) {
    return value;
  }
    

  if (!conversions[targetUnit].hasOwnProperty(sourceUnit)) {
    return value;
  }
  
  var converted = conversions[targetUnit][sourceUnit] * value;
  
  if (precision) {
    const new_precision = Math.pow(10, parseInt(precision) || 5);
    return Math.round(converted * new_precision) / new_precision;
  }
  
  return converted;
}