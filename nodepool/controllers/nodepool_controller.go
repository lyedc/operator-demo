/*
Copyright 2023.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package controllers

import (
	"context"
	"fmt"
	"github.com/go-logr/logr"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/workqueue"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"sigs.k8s.io/controller-runtime/pkg/source"
	"strings"
	"time"

	v1 "k8s.io/api/core/v1"
	"k8s.io/api/node/v1beta1"
	"k8s.io/client-go/tools/record"

	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	nodesv1 "github.com/mohuishou/blog-code/k8s-operator/03-node-pool-operator/api/v1"
)

// NodePoolReconciler reconciles a NodePool object
type NodePoolReconciler struct {
	client.Client
	Scheme   *runtime.Scheme
	Log      logr.Logger
	Recorder record.EventRecorder
}

const nodeFinalizer = "node.finalizers.node-pool.lailin.xyz"

//+kubebuilder:rbac:groups=nodes.lailin.xyz,resources=nodepools,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=nodes.lailin.xyz,resources=nodepools/status,verbs=get;update;patch
//+kubebuilder:rbac:groups=nodes.lailin.xyz,resources=nodepools/finalizers,verbs=update

// Reconcile is part of the main kubernetes reconciliation loop which aims to
// move the current state of the cluster closer to the desired state.
// TODO(user): Modify the Reconcile function to compare the state specified by
// the NodePool object against the actual cluster state, and then
// perform operations to make the cluster state reflect the state specified by
// the user.
//
// For more details, check Reconcile and its Result here:
// - https://pkg.go.dev/sigs.k8s.io/controller-runtime@v0.11.0/pkg/reconcile
func (r *NodePoolReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	_ = log.FromContext(ctx)
	r.Log.Info("resource change:%s-%s", req.Namespace, req.Name)
	pool := &nodesv1.NodePool{}
	// 注意这个namespaceName对象，后面是要get的对象
	if err := r.Get(ctx, req.NamespacedName, pool); err != nil {
		// 当对象已经被删除了。后面的相关和这个对象有关的代码都不能执行了。。。
		if client.IgnoreNotFound(err) == nil {
			log.Log.Info("资源已经被删除了。。。。。")
			r.Recorder.Event(pool, v1.EventTypeNormal, "change nodepool", fmt.Sprintf("the nodepool:%s is delete", req.Name))
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}
	r.Recorder.Event(pool, v1.EventTypeNormal, "change nodepool", fmt.Sprintf("the nodepool:%s is change", req.Name))

	var nodes v1.NodeList

	// 查看是否存在对应的节点，如果存在就给节点加上数据
	err := r.List(ctx, &nodes, &client.ListOptions{LabelSelector: pool.NodeLabelSelector()})
	if client.IgnoreNotFound(err) != nil {
		return ctrl.Result{}, err
	}
	// 如果删除资源的话，就要先判断是否有nodeFinzlizer，如果有就先处理旧资源，然后在删除资源
	if !pool.DeletionTimestamp.IsZero() {
		r.Log.Info("进入了删除逻辑了。。。")
		return ctrl.Result{}, r.nodeFinalizer(ctx, pool, nodes.Items)
	}
	// 如果删除时间戳为空说明现在不需要删除该数据，我们将 nodeFinalizer 加入到资源中
	if !containsString(pool.Finalizers, nodeFinalizer) {
		pool.Finalizers = append(pool.Finalizers, nodeFinalizer)
		if err := r.Client.Update(ctx, pool); err != nil {
			return ctrl.Result{}, err
		}
	}
	if len(nodes.Items) > 0 {
		r.Log.Info("find nodes, will merge data", "nodes", len(nodes.Items))
		pool.Status.Allocatable = v1.ResourceList{}
		pool.Status.NodeCount = len(nodes.Items)
		for _, n := range nodes.Items {
			tmp := n
			err := r.Patch(ctx, pool.Spec.ApplyNode(tmp), client.Merge)
			if err != nil {
				return ctrl.Result{}, err
			}
			// 更新status字段中的内容
			for name, quantity := range n.Status.Allocatable {
				q, ok := pool.Status.Allocatable[name]
				if ok {
					q.Add(quantity)
					pool.Status.Allocatable[name] = q
					continue
				}
				pool.Status.Allocatable[name] = quantity
			}
		}
	}
	runtimeClass := &v1beta1.RuntimeClass{}
	// 获取执行的runtime对象。第二个参数就是获取一个对象。是一个namespace和name的对象
	// ObjectKeyFromObject获取到的就是一个request
	err = r.Get(ctx, client.ObjectKeyFromObject(pool.RuntimeClass()), runtimeClass)
	if client.IgnoreNotFound(err) != nil {
		return ctrl.Result{}, err
	}
	// 如果没有就创建这个runtimeClass
	if runtimeClass.Name == "" {
		runtimeClass = pool.RuntimeClass()
		// 给资源设定OwnerReference角色,用于自助的删除runtimeclass对象。
		err = ctrl.SetControllerReference(pool, runtimeClass, r.Scheme)
		if err != nil {
			return ctrl.Result{}, err
		}
		err := r.Create(ctx, runtimeClass)
		if err != nil {
			return ctrl.Result{}, err
		}
	}
	// 如果存在就更新
	// client.Merge 是操作patch的时候进行的patch方式，包含了3中方式
	err = r.Client.Patch(ctx, pool.RuntimeClass(), client.Merge)
	if client.IgnoreNotFound(err) != nil {
		return ctrl.Result{}, err
	}
	pool.Status.Status = 200
	err = r.Status().Update(ctx, pool)
	// TODO(user): your logic here
	return ctrl.Result{}, nil
}

func (r *NodePoolReconciler) nodeFinalizer(ctx context.Context, pool *nodesv1.NodePool, nodes []v1.Node) error {
	// 不为空就说明进入到预删除流程
	for _, n := range nodes {
		n := n

		// 更新节点的标签和污点信息
		err := r.Update(ctx, pool.Spec.CleanNode(n))
		if err != nil {
			return err
		}
	}
	// 预删除执行完毕，移除 nodeFinalizer
	pool.Finalizers = removeString(pool.Finalizers, nodeFinalizer)
	// 更新资源。
	updateErr := r.Client.Update(ctx, pool)
	if updateErr != nil {
		log.Log.Info("错误了......")
		log.Log.Info(updateErr.Error())
	} else {
		log.Log.Info("我还是可以更新的，现在只是预删除阶段。。。")
	}

	return updateErr
}

func removeString(slice []string, s string) (result []string) {
	for _, item := range slice {
		if item == s {
			continue
		}
		result = append(result, item)
	}
	return
}

// 辅助函数用于检查并从字符串切片中删除字符串。
func containsString(slice []string, s string) bool {
	for _, item := range slice {
		if item == s {
			return true
		}
	}
	return false
}

// SetupWithManager sets up the controller with the Manager.
func (r *NodePoolReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&nodesv1.NodePool{}).Watches(&source.Kind{Type: &v1.Node{}}, handler.Funcs{UpdateFunc: r.nodeUpdateHandler}).
		Complete(r)
}

// 注意这里的 q 是在controller中的watch方法中传递过去的，但是最终还是controller中的q，也就是自定义的crd的controller，
//所以这里放的一定是crd的资源，不能是要watch的资源，只能是当watch的资源发生变化是触发 crd资源的更新，变动。
// 注意传入的是： c.Queue   controller中有两个queue
func (r *NodePoolReconciler) nodeUpdateHandler(e event.UpdateEvent, q workqueue.RateLimitingInterface) {
	r.Log.Info("node 信息改变了。。。")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	r.Log.Info(e.ObjectOld.GetName())
	r.Log.Info("获取node的名字")
	oldPool, err := r.getNodePoolByLabels(ctx, e.ObjectOld.GetLabels())
	if err != nil {
		r.Log.Error(err, "get node pool err")
	}
	if oldPool != nil {
		q.Add(reconcile.Request{
			NamespacedName: types.NamespacedName{Name: oldPool.Name},
		})
	}

	newPool, err := r.getNodePoolByLabels(ctx, e.ObjectNew.GetLabels())
	if err != nil {
		r.Log.Error(err, "get node pool err")
	}
	if newPool != nil {
		q.Add(reconcile.Request{
			NamespacedName: types.NamespacedName{Name: newPool.Name},
		})
	}
}



/*
   aaa: bbb
   beta.kubernetes.io/arch: amd64
   beta.kubernetes.io/os: linux
   kubernetes.io/arch: amd64
   kubernetes.io/hostname: gs-server-6783
   kubernetes.io/os: linux
   node-pool.lailin.xyz/master: '8'
   node-pool.lailin.xyz/test: '2'
   node-role.kubernetes.io/master: ''   主要是监听这个label没有这个lable就不会进行到队列中。。。


*/


func (r *NodePoolReconciler) getNodePoolByLabels(ctx context.Context, labels map[string]string) (*nodesv1.NodePool, error) {
	pool := &nodesv1.NodePool{}
	for k := range labels {
		ss := strings.Split(k, "node-role.kubernetes.io/")
		if len(ss) != 2 {
			continue
		}
		err := r.Client.Get(ctx, types.NamespacedName{Name: ss[1]}, pool)
		if err == nil {
			return pool, nil
		}

		if client.IgnoreNotFound(err) != nil {
			return nil, err
		}
	}
	return nil, nil
}
