import styled from "styled-components";

export const Container = styled.div`
  position: relative;
  display: flex;
  flex-direction: row;
  main{
    padding-right: 10px;
  }
  @media (max-width: 996px){
    margin: 0;
    margin-top: 0;
    width: 100%;
    max-width: 100%;
    padding: 0;
    main{
      padding-right: 0;
    }
    .markdown{
      > h2, > p {
        padding: 0 var(--ifm-spacing-horizontal);
      }
    }
    
  }
`

export const Nav = styled.div`

`