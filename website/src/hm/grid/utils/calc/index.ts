import {conversions} from './constant';
import {convert} from './convert'

export {valueOf} from './valueOf';

export function calc(precision: string) {
  const units = Object.keys(conversions);
  let exprecision = precision;
  let resultUnit = "";
  units.map((unit) => {
    if (precision.indexOf(unit) >= 0) {
      const reg = new RegExp(unit, 'g');
      if (!resultUnit) {
        resultUnit = unit;
        exprecision = exprecision.replace(reg, "");
      } else {
        const unitReg = new RegExp(`([0-9]*${unit})`, 'g');
        exprecision.match(unitReg)?.map((str) => {
          const value = str.replace(reg, "");
          const res = convert(Number(value), unit, resultUnit) || value;
          const strReg = new RegExp(str, 'g')
          exprecision = exprecision.replace(strReg, res as string);
        });
      }
    }
  });
  return eval(exprecision);
}