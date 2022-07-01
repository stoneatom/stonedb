import { ReactNode } from "react";

export interface IImage {
  src: string;
  className?: string;
  children?: ReactNode;
  width?: number;
  height?: number;
  alt?: string;
  to?: string
}
