
import React from 'react';
import type {Props} from '@theme/DocItem';


export default function DocItem(props: Props): JSX.Element {
  const {content: DocContent, versionMetadata} = props;

  return (
    <>
      <h1>888</h1>
      <DocContent />
    </>
  );
}
