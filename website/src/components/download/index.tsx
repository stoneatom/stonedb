import React, { useEffect } from 'react';
import { Button, Space } from 'antd';
import { DownloadOutlined, RightOutlined } from '@ant-design/icons';
import Translate from '@docusaurus/Translate';
import { useHistory, useLocation } from '@docusaurus/router';
import {loadScript} from '@site/src/utils';
import BgDownload from '@site/static/resource/BgDownload.png';
import { Context } from '../styles';
import { Item, DownLoadBtn, PanelWrap } from './styles';

const Download: React.FC = (props: any) => {
  const strs = props.children.split('\n');
  const history = useHistory();
  const location = useLocation()
  const renderAd = async () => {    
    // 实例化 PAG
    const PAG = await (window as any).libpag.PAGInit();
    // 获取 PAG 素材数据
    const buffer = await fetch("https://static.stoneatom.com/assets/stonedb.pag").then(
      (response) => response.arrayBuffer()
    );
    // 加载 PAG 素材为 PAGFile 对象
    const pagFile = await PAG.PAGFile.load(buffer);
    // 将画布尺寸设置为 PAGFile的尺寸
    const canvas = document.getElementById("pag");
    canvas.width = pagFile.width();
    canvas.height = pagFile.height();
    // 实例化 PAGView 对象
    const pagView = await PAG.PAGView.init(pagFile, canvas);
    pagView.setRepeatCount(0);
    // 播放 PAGView
    await pagView.play();
    
  }

  const gotoDownload = () => {
    history.push('docs/download');
  }

  const gotoStart = () => {
    history.push('docs/getting-started/quick-start');
  }

  useEffect(() => {
    if(location.pathname === '/' || location.pathname === '/zh/') {
      loadScript('//static.stoneatom.com/libpag.4.0.5.min.js' ,  {id: 'libpag'}).then((res) => {
        renderAd();
      });
    }
    
  }, [location.pathname]);

  return (
    <PanelWrap bg={BgDownload}>
    <Context>
      <Space size={10}>
        <Item>
          <h2>{strs[0]}</h2>
          <p>{strs[1]}</p>
          <Space>
            <DownLoadBtn size="large" type="primary" onClick={gotoDownload}>
              <b><DownloadOutlined /></b>
              <Translate id="home.download.btn">下载</Translate>
            </DownLoadBtn>
            <Button type="text" onClick={gotoStart}>
              <Translate id="home.download.start">快速上手</Translate>
              <RightOutlined />
            </Button>
          </Space>
        </Item>
        <Item>
          <canvas className="canvas" id="pag"></canvas>
          {/* <script src="https://unpkg.com/libpag@latest/lib/libpag.min.js"></script> */}
        </Item>
      </Space>
    </Context>
    </PanelWrap>
  );
};

export default Download;