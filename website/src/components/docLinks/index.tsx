import React, { useEffect, useState } from 'react';
import {unified} from 'unified';
import remarkParse from 'remark-parse';
import { v4 as uuidv4 } from 'uuid';
import {pickWhen} from '@site/src/utils';
import MDXA from '@site/src/theme/MDXComponents/A';
import {First, Second, Third, Fourth} from './styles';

const DocLinks: React.FC<any> = ({children, type}) => {
  const [list, setList] = useState(null);
  function init() {
    const node = unified().use(remarkParse).parse(children);
    const reduceParagraph = (list: any[]) => {
      if(!list.length) {
        return [];
      }
      return pickWhen('paragraph', (acc, cur) => {
        const {url, children, title} = cur.children[0];
        acc.push({
          title,
          url,
          text: children[0].value
        });
        return acc;
      }, list);
    }
    const reduceList = (list: any[]) => {
      return pickWhen('list', (acc, cur) => {
        const children = pickWhen('listItem', (acc, cur) => {
          let children = [];
          const ps = cur.children.filter(({type}) => type === 'paragraph');
          const lists = cur.children.filter(({type}) => type === 'list');
          if(ps?.length === lists?.length && lists?.length === 1) {
            const {url, title, ...current} = ps[0]?.children[0];
            children.push({
              title,
              url,
              text: current.children[0].value,
              children: reduceList(lists),
            });
          } else {
            children = reduceParagraph(cur.children)
          }
          return acc.concat(children);
        }, cur.children);
        return acc.concat(children);
      }, list);
    }
    const list = reduceList([node]);
    setList(list)
  }
  
  useEffect(() => {
    init();
  }, []);

  if(type && type === 'fourth') {
    return (
      <Fourth>
        {
          list && list.length && list.map(({text, url, title}) => (
            <MDXA to={url} title={title} key={uuidv4()}>{text}</MDXA>
          ))
        }
      </Fourth>
    )
  }

  return (
    <First>
      {
        list && list.length && list.map(({children, text, url, title}) => {
          return (
            <div key={uuidv4()}>
              <MDXA to={url} title={title}>{text}</MDXA>
              {
                children && children.length ? (
                  <Second>
                    {
                      children.map(({children, text, url, title}) => (
                        <div key={uuidv4()}>
                          <MDXA to={url} title={title}>{text}</MDXA>
                          {
                            children && children.length ? (
                              <Third>
                                {
                                  children.map(({text, url, title}) => (
                                    <MDXA to={url} title={title} key={uuidv4()}>{text}</MDXA>
                                  ))
                                }
                              </Third>
                            ) : null
                          }
                        </div>
                      ))
                    }
                  </Second>
                ) : null
              }
            </div>
          )
        })
      }
    </First>
  )

  // return (
  //   <First>
  //     {
  //       list && list.length && list.map(({children, text, url, title}) => {
  //         return (
            // <Col key={uuidv4()}>
              // <MDXA to={url} title={title} key={uuidv4()}>{text}</MDXA>
              // <Row>
                // {
                //   children && children.length && children.map(({text, url, title}) => {
                //     return (
                //       <Col key={uuidv4()}>
                //         <MDXA  to={url} title={title}>{text}</MDXA>
                //       </Col>
                //     )
                //   })
                // }
              // </Row>
            // </Col>
  //         )
  //       })
  //     }
  //   </First>
  // );
};
export default DocLinks;