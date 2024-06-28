import { TargetOptions } from '@angular-builders/custom-webpack';




export default (targetOptions: TargetOptions, indexHtml: string) => {
  const djangoScriptTemplate = (scriptName: string) => `<script type="text/javascript" src="{% static "js/${scriptName}" %}"></script>`;
  const djangoCSSTemplate = (styleName: string) => `<link rel="stylesheet" href="{% static "js/${styleName}" %}">`;

  const i = indexHtml.indexOf('</body>');
  const scriptNames = ['main', 'polyfills', 'runtime', 'vendor'];
  const styleNames = ['style'];

  let result = '{% load static  %}\n';
  for (const styleName of styleNames){
    if (indexHtml.indexOf(`href="${styleName}`) > 0){
      const index = indexHtml.indexOf(`href="${styleName}`) + `href="${styleName}`.length;
      const restIndexHtml = indexHtml.slice(index);
      const scriptFullName = styleName + restIndexHtml.slice(0, restIndexHtml.indexOf('"'));
      result += djangoCSSTemplate(scriptFullName) + '\n';
    }
  }
  result += '<app-root></app-root>\n';
  for (const scriptName of scriptNames){
    if (indexHtml.indexOf(`src="${scriptName}`) > 0){
      const index = indexHtml.indexOf(`src="${scriptName}`) + `src="${scriptName}`.length;
      const restIndexHtml = indexHtml.slice(index);
      const scriptFullName = scriptName + restIndexHtml.slice(0, restIndexHtml.indexOf('"'));
      result += djangoScriptTemplate(scriptFullName) + '\n';
    }
  }
  console.log(result);
  return result;
};
