import axios from 'axios';

const instance = axios.create({
  timeout: 30 * 1000,
  headers: {
      'Access-Control-Allow-Origin': '*'
  }
});

export async function postSubscribe(params: any): Promise<any> {
  const res = await instance.post('http://47.98.253.122/items/stoneDBSubscribes', params);
  return res.data;
}

export async function getSubscribes(): Promise<any> {
    const res = await instance.get('http://47.98.253.122/items/stoneDBSubscribes');
    return res.data;
  }