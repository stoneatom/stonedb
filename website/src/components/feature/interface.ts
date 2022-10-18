export interface IItem {
  icon: string;
  title: JSX.Element;
  desc?: JSX.Element;
  list?: JSX.Element[];
  values?: {
    title: string;
    list: {
      icon: string;
      title: JSX.Element;
    }[]
  };
}

export interface IMore {
  value: Pick<IItem, 'list' | 'values'>
}

export type SMore = IMore['value'] & {open: boolean};