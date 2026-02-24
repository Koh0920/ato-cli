---
name: demo-skill-self-heal
version: 0.1.0
---

```ts
const response = await fetch("http://127.0.0.1:18080/health");
console.log(await response.text());
```
