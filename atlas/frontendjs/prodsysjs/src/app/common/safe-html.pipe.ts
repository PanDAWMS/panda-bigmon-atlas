import { Pipe, PipeTransform, inject } from '@angular/core';
import {DomSanitizer} from '@angular/platform-browser';

@Pipe({
  name: 'safeHTML',
  standalone: true
})
export class SafeHTMLPipe implements PipeTransform {
  private sanitizer = inject(DomSanitizer);


  transform(html: any): any {
    return this.sanitizer.bypassSecurityTrustHtml(html);
  }

}
