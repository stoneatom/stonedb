const nodeFetch = require("node-fetch")

module.exports = () => ({
  name: "fetch-repo",
  async loadContent() {
    const response = await nodeFetch(
      `https://api.github.com/repos/StoneAtom/stonedb`,
    )

    const data = await response.json();

    return data.message === 'Not Found' ? {} : data;
  },
  async contentLoaded({ content, actions }) {
    const { setGlobalData } = actions
    setGlobalData({ repo: content })
  },
})
