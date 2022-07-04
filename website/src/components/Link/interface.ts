import { ReactNode } from "react";
import { IBase } from "../default.interface";

export interface ILink extends IBase {
  children: ReactNode;
  to?: string;
  icon?: ReactNode;
}
