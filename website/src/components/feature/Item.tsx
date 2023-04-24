import React, { useState } from 'react';
import Translate from '@docusaurus/Translate';
import {IItem} from './interface';
import { ItemWrap, ItemIcon, P, ItemContext, Button, ButtonIcon, ListWrap, MostWrap, ValueWrap, ValueIcon } from "./styles";

export const Item: React.FC<IItem> = ({title, desc, icon, list, values}) => {
  const [open, setOpen] = useState(false);
  const onToggle = () => {
    setOpen((state) => !state);
  }
  return (
    <ItemWrap open={open}>
      <ItemContext>
        <ItemIcon name={icon} />
        <P>
          <b>{title}</b>
          <span>{desc}</span>
        </P>
      </ItemContext>
      <MostWrap>
        <ListWrap >
          {
            list?.map((data: JSX.Element, index: number) => (
              <li key={index}>{data}</li>
            ))
          }
        </ListWrap>
        <dl>
          <dt>{values?.title}</dt>
          <dd>
            {
              values?.list?.map(({icon, title}: any, index: number) => (
                <ValueWrap key={index}>
                  <ValueIcon name={icon} />
                  <p>{title}</p>
                  <span>{desc}</span>
                </ValueWrap>
              ))
            }
          </dd>
        </dl>
      </MostWrap>
      <Button onClick={onToggle}>
        {open ? <Translate id="home.feature.close">收起</Translate> : <Translate id="home.feature.open">展开</Translate>}
        <ButtonIcon open={open} name="BottomOutlined" />
      </Button>
    </ItemWrap>
  )
}