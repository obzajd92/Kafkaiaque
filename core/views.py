from django.shortcuts import render, redirect, get_object_or_name
from core.models_sqla import Item
from core.kafka import publish_crud_event

def add_item(request):
    if request.method == 'POST':
        name = request.POST.get('name')
        description = request.POST.get('description')
        
        # 1. Save to DB via SQLAlchemy
        new_item = Item(name=name, description=description)
        request.db.add(new_item)
        request.db.flush() # Flushes to get the new DB ID before committing
        
        # 2. Publish Event to Kafka
        publish_crud_event(action="create", item_id=new_item.id, item_name=new_item.name)
        
        return redirect('item_list')
        
    return render(request, 'core/add_item.html')

def delete_item(request, item_id):
    # 1. Fetch and Delete from DB
    item = request.db.query(Item).filter(Item.id == item_id).first()
    if item:
        item_name = item.name
        request.db.delete(item)
        
        # 2. Publish Event to Kafka
        publish_crud_event(action="delete", item_id=item_id, item_name=item_name)
        
    return redirect('item_list')
