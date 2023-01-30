import React, {useRef, useState, useEffect} from 'react';
import { useInView } from 'react-intersection-observer';
import {IStep} from './interface';
import {StepWrap, StepItemWrap, StepContext, ButtonWrap, ButtonIcon, Logo} from './styles';

export const Step: React.FC<IStep> = ({dataSource, value}) => {
  const [scroll, setScroll] = useState(0);
  const { ref, inView } = useInView({
    threshold: 0.8,
  });

  const checkActive = (time: string) => {
    const res = time?.replace(/[年|月]/g, '-').replace('日', '');
    return res === value;
  }

  const checkDisable = (time: string) => {
    const res = time.replace(/[年|月]/g, '-').replace('日', '');
    const current = new Date(res).getTime();
    return isNaN(current) ? true : current > new Date(value as string).getTime()!;
  }

  const isLast = (index: number) => {
    return index === dataSource.length - 1;
  }

  const scrollPre = () => {
    setScroll((scroll) => scroll -= 320)
  }

  const scrollNext = () => {
    setScroll((scroll) => scroll += 320)
  }
 
  return (
    <StepWrap>
      <StepContext scroll={scroll} counts={dataSource?.length ?? 0}>
        <div className='wrap'>
        {
          dataSource.map(({title, desc, time, list}, index: number) => {
            const active = checkActive(time);
            return (
              <StepItemWrap 
                active={active} 
                disable={checkDisable(time)} 
                key={time} 
                ref={isLast(index) ? ref : null}
              >
                <dl>
                  <dt>{title}{active ? <Logo name="LogoFilled" /> :null}</dt>
                  <dd>
                    {
                      desc ? (<p>{desc}</p>) : null
                    }
                    <ol>
                      {
                        list.map((item) => (
                          <li key={item}>{item}</li>
                        ))
                      }
                    </ol>
                    <span>{time}</span>
                  </dd>
                </dl>
              </StepItemWrap>
            )
          })
        }
        </div>
        {
          scroll > 0 ? (
            <ButtonWrap type="left" onClick={scrollPre}>
              <ButtonIcon name="BottomOutlined" />
            </ButtonWrap>
          ) : null
        }
        {
          inView ? (<div></div>) : (
            <ButtonWrap type="right" onClick={scrollNext}>
              <ButtonIcon name="BottomOutlined" />
            </ButtonWrap>
          )
        }
        
      </StepContext>
    </StepWrap>
  )
}