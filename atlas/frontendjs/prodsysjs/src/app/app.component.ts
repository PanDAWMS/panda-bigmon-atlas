import { Component, inject } from '@angular/core';
import {NavigationCancel, NavigationEnd, NavigationError, NavigationStart, Router, RouterEvent} from '@angular/router';
import {AllCommunityModule, ModuleRegistry, provideGlobalGridOptions} from "ag-grid-community";

@Component({
    selector: 'app-root',
    templateUrl: './app.component.html',
    styleUrls: ['./app.component.css'],
    standalone: false
})

export class AppComponent {
  private router = inject(Router);

  title = 'ngProdSys';

  loading = true;

  constructor() {
    const router = this.router;

    ModuleRegistry.registerModules([AllCommunityModule]);
    provideGlobalGridOptions({ theme: "legacy"});
    router.events.subscribe((routerEvent) => {
      this.checkRouterEvent(routerEvent);
    });
  }

  checkRouterEvent(routerEvent: any): void {
    if (routerEvent instanceof NavigationStart) {
      this.loading = true;
    }

    if (routerEvent instanceof NavigationEnd ||
      routerEvent instanceof NavigationCancel ||
      routerEvent instanceof NavigationError) {
      this.loading = false;
    }
  }

}
