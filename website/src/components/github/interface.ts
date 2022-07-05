export interface IFork{
  value: string;
}

export interface IStar{
  value: string;
}


export interface IGithub{
  forks: string;
  star: string;
  children?: any;
  className?: string;
}
