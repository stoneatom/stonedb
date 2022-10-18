import React from 'react';
import { useHistory } from '@docusaurus/router';
import { IconFont } from '../iconFont';
import {ILink} from './interface';
import {LinkStyle, LinkIconStyle, LinkBtnStyle, LinkSocialStyle} from './styles';


const checkInterLink = (to: string) => {
  return to ? to.indexOf('://') < 0 : true
}

export const Link: React.FC<ILink> = ({to, children, className}) => {
  const history = useHistory();
  const isInterLink = checkInterLink(to);
  let options = isInterLink ? {
    onClick: () => {
      history.push(to);
    }
  } : {
    href: to,
    target:"_blank"
  }
  return (
    <LinkStyle 
      {...options}
      className={className}
    >
      {children}
    </LinkStyle>
  )
}

export const LinkIcon: React.FC<ILink> = ({to, children, className}) => {
  const history = useHistory();
  const isInterLink = checkInterLink(to);
  let options = isInterLink ? {
    onClick: () => {
      history.push(to);
    }
  } : {
    href: to,
    target:"_blank"
  }
  return (
    <LinkIconStyle 
      {...options}  
      className={className}
    >
      {children as string}
      <IconFont type="icon-a-bianzu301" />
    </LinkIconStyle>
  )
}

export const LinkBtn: React.FC<ILink> = ({to, children}) => {
  const history = useHistory();
  return (
    <LinkBtnStyle onClick={() => history.push(to)}>
      {children as string}
      <IconFont type="icon-a-bianzu301" />
    </LinkBtnStyle>
  );
}

export const LinkSocial: React.FC<ILink> = ({to, icon, children, className}) => {
  const history = useHistory();
  const isInterLink = checkInterLink(to);
  let options = isInterLink ? {
      onClick: () => {
        history.push(to);
      }
    } : {
      href: to,
      target:"_blank"
    }
  return (
    <LinkSocialStyle 
      {...options} 
      className={className}
    >
      <IconFont type={icon} />
      <span>{children}</span>
      <IconFont type="icon-a-bianzu301" />
    </LinkSocialStyle>
  )
}