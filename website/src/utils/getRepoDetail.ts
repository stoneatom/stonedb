import axios from 'axios';

const instance = axios.create({
  timeout: 30 * 1000,
});

export async function getRepoDetail(name: string): Promise<any> {
  try{
    const res = await instance.get('https://api.github.com/repos/' + name);
    return res.data;
  } catch(err) {
    return {}
  }
}
