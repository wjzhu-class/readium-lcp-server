import { NgModule }                 from '@angular/core';
import { RouterModule, Routes }     from '@angular/router';

import { PurchaseAddComponent } from './purchase-add.component';
import { PurchaseEditComponent } from './purchase-edit.component';
import { PurchaseStatusComponent } from './purchase-status.component';
import { PurchaseListComponent } from './purchase-list.component';

const purchaseRoutes: Routes = [
    { path: 'purchases/:id/renew', component: PurchaseEditComponent },
    { path: 'purchases/:id/status', component: PurchaseStatusComponent },
    { path: 'purchases/add', component: PurchaseAddComponent },
    { path: 'purchases', component: PurchaseListComponent }
];

@NgModule({
  imports: [
    RouterModule.forChild(purchaseRoutes)
  ],
  exports: [
    RouterModule
  ]
})

export class PurchaseRoutingModule { }
