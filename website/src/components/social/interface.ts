import { ReactNode } from "react";

export interface ISocial {
  title: ReactNode;
  value: string;
  children: ReactNode;
  to?: string;
}
