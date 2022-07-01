import React from 'react';
import {Form} from './form';
import {Message} from './message';
import {Tip} from './tip';
import { ISubscribeForm } from './interface';
import {IBase} from '../default.interface';


export class SubscribeMail extends React.Component {

  static Form (props: ISubscribeForm) {
    return (
      <Form {...props} />
    )
  }

  static Message({className}: IBase) {
    return (
      <Message className={className} />
    )
  }

  static Tip({className}: IBase) {
    return (
      <Tip className={className} />
    )
  }

  render() {
    return null;
  }
}
