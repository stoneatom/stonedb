import React from 'react';
import {getRepoDetail} from '@site/src/utils';
import {Fork} from './fork';
import {Star} from './star';
import {IGithub} from './interface';
import GithubStyle from './styles';

const GithubContext = React.createContext<IGithub>({
  star: '',
  forks: ''
});


export class Github extends React.Component {
  constructor(props: any) {
    super(props);
    this.state = {
      star: '',
      forks: ''
    };
    this.getData();
  }

  async getData() {
    const {forks_count, stargazers_count} = await getRepoDetail('StoneAtom/stonedb');
    this.setState({
      star: stargazers_count,
      forks: forks_count
    })
  }

  static Fork() {
    return (
      <GithubContext.Consumer>
        {
          ({forks}) => (
            <Fork value={forks} />
          )
        }
      </GithubContext.Consumer>
    )
  }

  static Star() {
    return (
      <GithubContext.Consumer>
        {
          ({star}) => (
            <Star value={star} />
          )
        }
      </GithubContext.Consumer>
    )
  }

  render() {
    return (
      <GithubContext.Provider value={this.state as IGithub}>
        <GithubStyle className={this.props.className}>
          {this.props.children}
        </GithubStyle>
      </GithubContext.Provider>
    );
  }
}
