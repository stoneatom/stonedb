
/**
 * 仅触发一次事件
 *
 * @private
 *
 * @param node      DOM 节点
 * @param name      事件名称
 * @param callback  回调函数
 */
function once(
  node: HTMLScriptElement,
  name: string,
  fn: EventListener,
  options?: boolean | AddEventListenerOptions,
) {
  function listener(event: Event) {
    node.removeEventListener(name, listener);
    fn(event);
  }
  node.addEventListener(name, listener, options);
}

/**
 * 定义IE浏览器的 HTMLScriptElement 属性
 */
export interface MSHTMLScriptElement extends HTMLScriptElement {
  readyState: 'loaded' | 'complete';
  onreadystatechange: ((this: Window, ev: ProgressEvent<Window>) => any) | null;
}

/**
 * 监听脚本加载事件 - IE 兼容
 *
 * @private
 *
 * @param node     DOM 节点
 * @param callback 回调函数
 */
function listenWithIE(node: MSHTMLScriptElement, callback: () => void) {
  node.onreadystatechange = function () {
    if (node.readyState === 'loaded' || node.readyState === 'complete') {
      node.onreadystatechange = null;
      callback();
    }
  };
}

/**
 * 监听脚本加载事件 - 其他浏览器
 *
 * @private
 *
 * @param node     DOM 节点
 * @param callback 回调函数
 */
function listen(
  node: HTMLScriptElement,
  onSuccess: EventListener,
  onError: EventListener,
) {
  once(node, 'load', onSuccess);
  once(node, 'error', onError);
}

/**
 * 白名单属性
 */
export type AllowListsAttributes = {
  /** 设置 id */
  id: string;
  /** 脚本类型 */
  type?: string;
  /** 异步加载 */
  async?: boolean;
  /** 延迟加载 */
  defer?: boolean;
  /** 跨域属性 */
  crossOrigin?: string | null;
  /** Subresource Integrity (SRI) */
  integrity?: string;
  /** es6 模块回退 */
  noModule?: boolean;
};

// 允许的属性名单
const allowLists: Array<keyof AllowListsAttributes> = [
  'id',
  'type',
  'async',
  'defer',
  'crossOrigin',
  'integrity',
  'noModule',
];

/**
 * 动态加载脚本
 *
 * @param src    文件地址
 * @param attrs  可选的属性
 *
 * @returns dom 节点
 *
 * @example
 *
 * try {
 *  const src = 'https://cdn.bootcdn.net/ajax/libs/jquery/3.6.0/jquery.min.js'
 *
 *  await loadScript(src, { id: 'jquery.3.6.0.min' })
 * } catch {
 *   alert('加载失败')
 * }
 */
export function loadScript(
  src: string,
  attrs: Partial<AllowListsAttributes>,
): Promise<HTMLScriptElement> {
  return new Promise((resolve, reject) => {
    if(document.getElementById(attrs.id as string)) {
      resolve(document.getElementById(attrs.id as string)[0])
    }
    const scriptNode: HTMLScriptElement = document.createElement('script');
    scriptNode.id = attrs.id as string;

    // 监听事件
    if ((scriptNode as MSHTMLScriptElement).readyState) {
      listenWithIE(scriptNode as MSHTMLScriptElement, () => resolve(scriptNode));
    } else {
      listen(
        scriptNode,
        () => resolve(scriptNode),
        () => {
          reject();
          document.removeChild(document.getElementById(attrs.id as string) as Node);
        },
      );
    }

    // 设置白名单属性
    if (attrs) {
      allowLists.forEach(attrName => {
        if (attrName in attrs) {
          const value = attrs[attrName];

          if (typeof value === 'string') {
            scriptNode.setAttribute(attrName, value);
          } else if (typeof value === 'boolean') {
            // 布尔值与字符串的设置不太一样
            scriptNode[attrName as 'defer' | 'async' | 'noModule'] = value;
          }
        }
      });
    }

    // 设置资源地址
    scriptNode.src = src;

    // 插入到页面中
    document.body.appendChild(scriptNode);
  });
}

export function unLoadScript(progectId: string) {
  if (document.getElementById(progectId)) {
    const scriptNode: any = document.getElementById(progectId);
    document.body.removeChild(scriptNode)
  }
}
