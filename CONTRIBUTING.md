# How to contribute to Orion? #

Everyone is welcome to contribute to Orion! You can support our work by contributing code, improving documentation and testing as well as sharing with us how you are using Orion. Moreover, spreading the word about it as well as connecting us with potential users would be immensly valuable.

## Ways to contribute ##
We have listed four ways to contribute to Orion but get in touch if you have other ideas:
- Work on outstanding [issues](https://github.com/orion-search/orion/issues).
- Implement new components. For example, add a new metric or data source.
- Submit issues related to bugs or desired new features.
- Connecting us with potential users.

### Working on an outstanding issue ###
Would you like to work on an existing issue? That's great! Before writing any code, we strongly advise you to reply to the issue you plan to work on and provide the following details:
- Describe your solution to the issue.
- Add any references that will help us learn more about your proposed solution.

We aim to reply as soon as possible and guide you as best as we can. After agreeing on the scope of your contribution, you can start working on it:
- Fork the [repository](https://github.com/orion-search/orion) and clone it on your local disk.
- Create a new branch for your changes. **Do not** work directly on the `master` or `dev` branches.
- Develop and test your solution. Make sure it also passes existing tests. Document your contribution and use [`black`](https://github.com/psf/black) for code formatting.
- When you are happy with your changes, push your code and open a Pull Request (PR) describing your solution. If it is still work in progress, create a draft PR and add a **[WIP]** in front of its title. 
- Add me (@kstathou) as a reviewer. If I suggest some changes, work on your local branch and push them to your fork. Let me know when I should review the new additions and after my approval, you can merge your contribution to Orion's `dev` branch!

### Suggesting new features ###
We are actively developing Orion and your ideas can help us make it better! If you want to suggest a new feature, open a new issue and provide the following details:
- Motivation: Why do you think this feature is important and should be developed in Orion?
  - Is it related to a problem (not bug) with Orion? For example, is there a better way to do X, or is a data source with better coverage than what we use?
  - Is it a feature you saw somewhere else and it would be a valuable addition to Orion? Let us know where you found it!
- Describe the new feature.
- Add any references that might help us learn more about it.

### Reporting bugs ###
Orion is in beta so we except users to come across some bugs. If you found one, let us know by submitting a new issue with the title `Bug: descriptive_title_for_the_bug` and providing the following details:
- Your Python version.
- Let us know which task failed or didn't work as expected. Giving us a data sample to rerun the task would be very helpful.
- Full error message.

This guide was inspired by the [transformers](https://github.com/huggingface/transformers).
