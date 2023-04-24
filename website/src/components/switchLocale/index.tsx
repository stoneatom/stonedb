import React, { useEffect, useState } from 'react';
import useDocusaurusContext from '@docusaurus/useDocusaurusContext';
import { IconFont } from '../iconFont';
import {SelectStyle} from './styles';

export const SwitchLocal = () => {
  const {
    i18n: {currentLocale, defaultLocale, localeConfigs}
  } = useDocusaurusContext();
  const [locale, setLocale] = useState(defaultLocale);
  const changeLocale = (value: string) => {
    setLocale(value)
  }

  useEffect(() => {
    setLocale(currentLocale)
  }, []);
  
  return (
    <SelectStyle bordered={false} value={locale} onChange={changeLocale}>
      {
        Object.keys(localeConfigs).map((lan) => (
          <SelectStyle.Option value={lan} key={lan}><IconFont type="icon-a-bianzu9" />{localeConfigs[lan].label}</SelectStyle.Option>
        ))
      }
    </SelectStyle>
  )
}